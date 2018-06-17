/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Amqp;
using Amqp.Framing;
using System.Threading;
using System.Threading.Tasks;

namespace Examples.Async {

    class Drain {
        //
        // Sample invocation: Async.Drain.exe --broker localhost:5672 --task-duration 30 --address my-queue
        //                  : dotnet run -- --help
        //

        Options options;
        ReceiverLink receiver;
        int instance;
        List<Worker> workersIdle;
        List<Worker> workersBusy;
        List<Message> messageQueue;
        int statMsgInTotal;
        int statMsgToWorker;
        int statMsgRetired;
        bool timesUp; // wall clock expiration exit triggered
        bool msgCountSatisfied; // count completion exit triggered
        AsyncAutoResetEvent wake;
        Timer elapsedTmoTimer;
        Stopwatch wallClockTimer;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options_">parsed command line</param>
        /// <param name="instance_">identifier number</param>
        public Drain(Options options_, int instance_)
        {
            options = options_;
            instance = instance_;
            receiver = null;
            workersIdle = new List<Worker>();
            workersBusy = new List<Worker>();
            for (int i=0; i<options.TaskPoolSize; i++)
            {
                workersIdle.Insert(i, new Worker(this, i));
                workersBusy.Insert(i, null);
            }
            messageQueue = new List<Message>();
            statMsgInTotal = 0;
            statMsgToWorker = 0;
            statMsgRetired = 0;
            timesUp = false;
            msgCountSatisfied = false;
            wake = new AsyncAutoResetEvent();
            int timeout = int.MaxValue;
            if (!options.Forever)
                timeout = options.TestDuration * 1000;
            elapsedTmoTimer = new Timer(this.OnTimedTimeout, wake, timeout, 1000);
            wallClockTimer = new Stopwatch();
        }

        public void LoggerTrace(string ls)
        {
            Console.WriteLine(DateTime.Now.ToString("[hh:mm:ss.fff]") +
                " TRACE Instance:" + instance + " " + ls);
        }

        public void LoggerDebug(string ls)
        {
            Console.WriteLine(DateTime.Now.ToString("[hh:mm:ss.fff]") +
                " DEBUG Instance:" + instance + " " + ls);
        }

        public void LoggerInfo(string ls)
        {
            Console.WriteLine(DateTime.Now.ToString("[hh:mm:ss.fff]") + 
                " INFO Instance:" + instance + " " + ls);
        }

        /// <summary>
        /// Trigger the process exit from timeout event
        /// </summary>
        public void OnTimedTimeout(Object source)
        {
            if (options.LogDebug) LoggerDebug("Timeout expired");
            timesUp = true;
            if (wallClockTimer.IsRunning) wallClockTimer.Stop();
            receiver.SetCredit(options.CreditInitial, false);
            wake.Set();
        }

        /// <summary>
        /// Function to determine whether the process has begun shutdown.
        /// If this function returns true then do not inject new messages
        /// into the system.
        /// </summary>
        public bool ExitSignaled()
        {
            return msgCountSatisfied || timesUp;
        }


        /// <summary>
        /// Worker has finished processing a message.
        /// * Dispose of the message
        /// * Put the worker back on the available list
        /// * Detect when enough mesages have been processed to stop by-count
        /// * Trigger the main loop to process more messages
        /// * Release messages that arrive when we are shutting down
        /// </summary>
        /// <param name="worker"></param>
        /// <param name="msg"></param>
        public void WorkerDone(Worker worker, Message msg)
        {
            if (!ExitSignaled())
            {
                receiver.Accept(msg);
                lock (this)
                {
                    workersBusy.Insert(worker.Index, null);
                    workersIdle.Add(worker);
                }
                statMsgRetired++;
                msgCountSatisfied = options.Count > 0 && statMsgRetired >= options.Count;
                if (msgCountSatisfied)
                {
                    if (wallClockTimer.IsRunning) wallClockTimer.Stop();
                    receiver.SetCredit(options.CreditInitial, false);
                    if (options.LogDebug) LoggerDebug(string.Format("message count satisfied at {0} messages retired",
                        statMsgRetired));
                }
                if (options.LogTrace) LoggerTrace(string.Format("Rx Completion worker: {0}, messages retired {1}", 
                    worker.Index, statMsgRetired));
            }
            else
            {
                // message that was in processing is done but now we are shutting down
                if (!receiver.IsClosed)
                    receiver.Release(msg);
                if (options.LogTrace) LoggerTrace(string.Format("Rx Completion worker: {0}. Shutting down, message released.",
                    worker.Index));
            }
            wake.Set();
        }

        /// <summary>
        /// Message delivered by amqpdotnetlite via callback
        /// </summary>
        public void messageReceived(IReceiverLink rlink, Message message)
        {
            if (!ExitSignaled())
            {
                lock (this)
                {
                    messageQueue.Add(message);
                }
                statMsgInTotal++;
                if (options.LogTrace) LoggerTrace(string.Format("Rx callback from Lite messagesIn:{0}",
                    statMsgInTotal));
            }
            else
            {
                rlink.Release(message);
            }
            wake.Set();
        }

        /// <summary>
        /// Drain main loop.
        /// </summary>
        /// <returns></returns>
        async Task Run() {
            Connection connection = null;
            try
            {
                LoggerInfo(string.Format("Async.Drain starting. " +
                    "Url:{0}, address:{1}, credit:{2}, count:{3}, duration:{4}",
                    options.Url, options.Address, options.CreditInitial, options.Count, options.TestDuration));
                Address address = new Address(options.Url);
                connection = await Connection.Factory.CreateAsync(address);
                Session session = new Session(connection);
                receiver = new ReceiverLink(session, 
                                            "receiver-drain-" + options.Instances.ToString(), 
                                            options.Address);

                Random rnd = new Random();
                
                wallClockTimer.Start();
                receiver.Start(options.CreditInitial, (r, m) =>
                {
                    messageReceived(r, m); // this is the receive callback entry point
                });

                while (!ExitSignaled())
                {
                    await wake.WaitAsync(); // wait for work to do

                    // if possible and not overdoing it then give a message to a worker
                    while ((messageQueue.Count > 0 && workersIdle.Count > 0) && 
                           !(options.Count > 0 && statMsgToWorker >= options.Count))
                    {
                        Worker wrkr;
                        lock (this)
                        {
                            // get worker and move to busy list
                            wrkr = workersIdle[0];
                            workersIdle.RemoveAt(0);
                            workersBusy.Insert(wrkr.Index, wrkr);
                            // get message
                            wrkr.Msg = messageQueue[0];
                            messageQueue.RemoveAt(0);
                        }
                        // compute and set worker task 'processing time' in mS
                        wrkr.Delay = Convert.ToDouble(rnd.Next(options.TaskDelayMin, options.TaskDelayMax));
                        // run worker
                        if (options.LogTrace) LoggerTrace(string.Format("Launching worker: {0} with delay {1}",
                            wrkr.Index, wrkr.Delay));
                        wrkr.Run().Forget();
                        statMsgToWorker++;
                    }
                }
                LoggerInfo(string.Format("Instance {0} exiting. msgsIn= {1}, " +
                    "tasks={2}, msgsDone={3}; Performance: {4}",
                    instance, statMsgInTotal, statMsgToWorker, statMsgRetired,
                    Helpers.MessagesPerSecond(statMsgRetired, wallClockTimer.ElapsedMilliseconds)));

                // flush messages in intermediate queue
                foreach(Message m in messageQueue)
                {
                    receiver.Release(m);
                }

                // Closing the receiver immediately may hang. 
                // Give the receiver time to deliver prefetched messages
                // and to return any Released status we signal for them.
                await Task.Delay(500);

                receiver.Close();
                session.Close();
                connection.Close();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Drain Instance {0} Exception {1}.", 
                    instance, e);
                if (null != connection)
                    connection.Close();
            }
            return;
        }

        async Task WaitForCompletion(List<Task> taskList)
        {
            await Task.WhenAll(taskList.ToArray());
        }

        static public int Main(string[] args)
        {
            Options options = new Options(args);
            if (options.Instances == 0)
                return 0;

            List<Drain> drainList = new List<Drain>();
            List<Task> taskList = new List<Task>();
            for (int idx = 0; idx < options.Instances; idx++)
            {
                var thisDrain = new Drain(options, idx);
                var thisTask = Task.Run(thisDrain.Run);
                drainList.Add(thisDrain);
                taskList.Add(thisTask);
            }
            drainList[0].WaitForCompletion(taskList).Wait();

            // Produce a stats report aggregated across all instances.
            int tMessagesIn = 0;
            int tTasksLaunched = 0;
            int tMessagesDone = 0;
            long tMs = 0;
            for (int idx = 0; idx < options.Instances; idx++)
            {
                tMessagesIn += drainList[idx].statMsgInTotal;
                tTasksLaunched  += drainList[idx].statMsgToWorker;
                tMessagesDone += drainList[idx].statMsgRetired;
                tMs += drainList[idx].wallClockTimer.ElapsedMilliseconds;
            }
            tMs = tMs / options.Instances;
            drainList[0].LoggerInfo(string.Format("Async.Drain exit totals: " +
                "msgsIn= {0}, tasks={1}, msgsDone={2}; Performance: {3}",
                tMessagesIn, tTasksLaunched, tMessagesDone, 
                Helpers.MessagesPerSecond(tMessagesDone, tMs)));

            return 0;
        }
    }
}
