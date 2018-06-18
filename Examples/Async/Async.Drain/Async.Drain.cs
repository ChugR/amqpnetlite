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

#define USE_LITE_RECEIVER
//#define USE_FAKE_RECEIVER

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Amqp;
using Amqp.Framing;
using System.Threading;
using System.Threading.Tasks;

namespace Examples.Async {

    public class PerformanceStats {
        public long snapMs;
        public long lastMs;
        public int snapTotal;
        public int snapToWorker;
        public int snapRetired;
        public int lastTotal;
        public int lastToWorker;
        public int lastRetired;

        public PerformanceStats()
        {
            // this snapshot = current - last
            snapMs = 0;
            snapTotal = 0;
            snapToWorker = 0;
            snapRetired = 0;
            // starting point for next snapshot
            lastMs = 0;
            lastTotal = 0;
            lastToWorker = 0;
            lastRetired = 0;
        }
    }

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
        PerformanceStats totalStats;
        PerformanceStats intervalStats;
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
            totalStats = new PerformanceStats();
            intervalStats = new PerformanceStats();
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
            Console.WriteLine(DateTime.Now.ToString("[hh:mm:ss.ffffff]") +
                " TRACE Instance:" + instance + " " + ls);
        }

        public void LoggerDebug(string ls)
        {
            Console.WriteLine(DateTime.Now.ToString("[hh:mm:ss.ffffff]") +
                " DEBUG Instance:" + instance + " " + ls);
        }

        public void LoggerInfo(string ls)
        {
            Console.WriteLine(DateTime.Now.ToString("[hh:mm:ss.ffffff]") + 
                " INFO Instance:" + instance + " " + ls);
        }

        /// <summary>
        /// Take a snapsot of the performance stats
        /// </summary>
        public PerformanceStats PerformanceSnapshot()
        {
            long nowmS = wallClockTimer.ElapsedMilliseconds;
            int total = totalStats.lastTotal;
            int wrkr = totalStats.lastToWorker;
            int rtrd = totalStats.lastRetired;
            intervalStats.snapMs = nowmS - intervalStats.lastMs;
            intervalStats.snapTotal = total - intervalStats.lastTotal;
            intervalStats.snapToWorker = wrkr - intervalStats.lastToWorker;
            intervalStats.snapRetired = rtrd - intervalStats.lastRetired;
            intervalStats.lastMs = nowmS;
            intervalStats.lastTotal = total;
            intervalStats.lastToWorker = wrkr;
            intervalStats.lastRetired = rtrd;
            return intervalStats;
        }

        /// <summary>
        /// Trigger the process exit from timeout event
        /// </summary>
        public void OnTimedTimeout(Object source)
        {
            if (options.LogDebug) LoggerDebug("Timeout expired");
            timesUp = true;
            if (wallClockTimer.IsRunning) wallClockTimer.Stop();
            if (!options.AutoReceive)
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
                if (options.AutoReceive)
                    msg.Dispose();
                else
                    receiver.Accept(msg);

                lock (this)
                {
                    workersBusy.Insert(worker.Index, null);
                    workersIdle.Add(worker);
                }
                totalStats.lastRetired++;
                msgCountSatisfied = options.Count > 0 && totalStats.lastRetired >= options.Count;
                if (msgCountSatisfied)
                {
                    if (wallClockTimer.IsRunning) wallClockTimer.Stop();
                    if (!options.AutoReceive)
                        receiver.SetCredit(options.CreditInitial, false);
                    if (options.LogDebug) LoggerDebug(string.Format("message count satisfied at {0} messages retired",
                        totalStats.lastRetired));
                }
                if (options.LogTrace) LoggerTrace(string.Format("Rx Completion worker: {0}, messages retired {1}", 
                    worker.Index, totalStats.lastRetired));
            }
            else
            {
                // message that was in processing is done but now we are shutting down

                if (options.AutoReceive)
                    msg.Dispose();
                else
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
                totalStats.lastTotal++;
                if (options.LogTrace) LoggerTrace(string.Format("Rx callback from Lite messagesIn:{0}",
                    totalStats.lastTotal));
            }
            else
            {
                rlink.Release(message);
            }
            wake.Set();
        }

        public void fakeMessageReceived(Message message)
        {
            if (!ExitSignaled())
            {
                lock (this)
                {
                    messageQueue.Add(message);
                }
                totalStats.lastTotal++;
                if (options.LogTrace) LoggerTrace(string.Format("Rx callback from Lite messagesIn:{0}",
                    totalStats.lastTotal));
            }
            else
            {
                message.Dispose();
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
                    "Url:{0}, address:{1}, credit:{2}, count:{3}, duration:{4}, autoReceive:{5}",
                    options.Url, options.Address, options.CreditInitial, options.Count,
                    options.TestDuration, options.AutoReceive));
                Random rnd = new Random();
                Address address = null;
                Session session = null;
    
                if (!options.AutoReceive)
                {
                    address = new Address(options.Url);
                    connection = await Connection.Factory.CreateAsync(address);
                    session = new Session(connection);
                    receiver = new ReceiverLink(session, 
                                                "receiver-drain-" + options.Instances.ToString(), 
                                                options.Address);
                    wallClockTimer.Start();

                    receiver.Start(options.CreditInitial, (r, m) =>
                    {
                        messageReceived(r, m); // this is the receive callback entry point
                    });
                }
                else
                {
                    wallClockTimer.Start();
                    for (int i=0; i<options.CreditInitial/2; i++)
                    {
                        Message msg = new Message("abc");
                        fakeMessageReceived(msg);
                    }
                }

                while (!ExitSignaled())
                {
                    await wake.WaitAsync(); // wait for work to do

                    // if possible and not overdoing it then give a message to a worker
                    while ((messageQueue.Count > 0 && workersIdle.Count > 0) && 
                           !(options.Count > 0 && totalStats.lastToWorker >= options.Count))
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
                        totalStats.lastToWorker++;
                        // replenish if autoReceive
                        if (options.AutoReceive)
                        {
                            Message msg = new Message("abc");
                            fakeMessageReceived(msg);
                        }
                    }
                }
                LoggerInfo(string.Format("Instance {0} exiting. msgsIn= {1}, " +
                    "tasks={2}, msgsDone={3}; Performance: {4}",
                    instance, totalStats.lastTotal, totalStats.lastToWorker, totalStats.lastRetired,
                    Helpers.MessagesPerSecond(totalStats.lastRetired, wallClockTimer.ElapsedMilliseconds)));

                // flush messages in intermediate queue
                foreach(Message m in messageQueue)
                {
                    if (options.AutoReceive)
                        m.Dispose();
                    else
                        receiver.Release(m);
                }

                // Closing the receiver immediately may hang. 
                // Give the receiver time to deliver prefetched messages
                // and to return any Released status we signal for them.
                if (!options.AutoReceive)
                {
                    await Task.Delay(500);

                    receiver.Close();
                    session.Close();
                    connection.Close();
                }
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

        public void PrintAggregatedStats(object obj)
        {
            List<Drain> dList = (List<Drain>) obj;
            foreach (Drain d in dList)
            {
                PerformanceStats s = d.PerformanceSnapshot();
                d.LoggerInfo(string.Format("perf snapshot: " +
                "msgsIn= {0}, tasks={1}, msgsDone={2}; Performance: {3}",
                s.snapTotal, s.snapToWorker, s.snapRetired, 
                Helpers.MessagesPerSecond(s.snapRetired, s.snapMs)));
            }
        }

        static public int Main(string[] args)
        {
            Options options = new Options(args);
            if (options.Instances == 0)
                return 0;
            Timer intervalTimer;

            List<Drain> drainList = new List<Drain>();
            List<Task> taskList = new List<Task>();
            for (int idx = 0; idx < options.Instances; idx++)
            {
                var thisDrain = new Drain(options, idx);
                var thisTask = Task.Run(thisDrain.Run);
                drainList.Add(thisDrain);
                taskList.Add(thisTask);
            }

            intervalTimer = new Timer(
                drainList[0].PrintAggregatedStats, drainList, 
                options.StatsInterval * 1000, options.StatsInterval * 1000);

            drainList[0].WaitForCompletion(taskList).Wait();

            // Produce a stats report aggregated across all instances.
            int tMessagesIn = 0;
            int tTasksLaunched = 0;
            int tMessagesDone = 0;
            long tMs = 0;
            for (int idx = 0; idx < options.Instances; idx++)
            {
                tMessagesIn += drainList[idx].totalStats.lastTotal;
                tTasksLaunched  += drainList[idx].totalStats.lastToWorker;
                tMessagesDone += drainList[idx].totalStats.lastRetired;
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
