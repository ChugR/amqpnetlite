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
using System.Threading.Tasks;

namespace Examples.Async {
    public class Spout {
        //
        // Sample invocation: Async.Spout.exe --broker localhost:5672 --timeout 30 --address my-queue
        //

        Options options;
        int instance;
        int sentCount;
        Stopwatch stopwatch;

        public Spout(Options options_, int instance_)
        {
            options = options_;
            instance = instance_;
            sentCount = 0;
            stopwatch = new Stopwatch();
        }

        public string MessagesPerSecond(int totalMessages, long elapsedMS)
        {
            double nMsgs = Convert.ToDouble(totalMessages);
            double emS = Convert.ToDouble(elapsedMS);
            if (emS == 0.0) emS = 1.0;
            double secs =  emS / 1000.0;
            string result = string.Format("time(S): {0:N3}, msg/S: {1:N2}",
                secs, nMsgs / emS * 1000.0);
            return result;
        }

        public void LoggerInfo(string ls)
        {
            Console.WriteLine(DateTime.Now.ToString("[hh:mm:ss.fff]") + 
                " INFO Instance:" + instance + " " + ls);
        }

        async Task<int> Run() {
            const int ERROR_SUCCESS = 0;
            const int ERROR_OTHER = 2;

            LoggerInfo(string.Format("Async.Spout starting. Url:{0}, address:{1}",
                options.Url, options.Address));
            int exitCode = ERROR_SUCCESS;
            Connection connection = null;
            int callbackCount = 0;
            try
            {
                Address address = new Address(options.Url);
                connection = new Connection(address);
                Session session = new Session(connection);
                SenderLink sender = new SenderLink(session, "sender-spout-"+instance.ToString(), options.Address);
                // TODO: ReplyTo

                TimeSpan timespan = new TimeSpan(0, 0, options.Timeout);
                stopwatch.Start();
                for (int nSent = 0;
                    (0 == options.Count || nSent < options.Count) &&
                    (0 == options.Timeout || stopwatch.Elapsed <= timespan);
                    nSent++)
                {
                    string id = options.Id;
                    if (id.Equals(""))
                    {
                        Guid g = Guid.NewGuid();
                        id = g.ToString();
                    }
                    id += ":" + nSent.ToString();

                    Message message = new Message(options.Content);
                    message.Properties = new Properties() { MessageId = id };
                    if (options.Durable || options.Ttl >= 0)
                    {
                        message.Header = new Header();
                        if (options.Ttl >= 0)
                        {
                            message.Header.Ttl = (uint)options.Ttl;
                        }
                        if (options.Durable)
                        {
                            message.Header.Durable = true;
                        }
                    }
                    OutcomeCallback callback = (l, msg, o, s) => { callbackCount++; };
                    if (options.Synchronous)
                    {
                        sender.Send(message);
                    }
                    else
                    {
                        sender.Send(message, callback, null);
                    }
                    sentCount++;
                    if (options.Delay > 0)
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(Convert.ToDouble(options.Delay)));
                    }
                    if (options.Print)
                    {
                        Console.WriteLine("Message(Properties={0}, ApplicationProperties={1}, Body={2}",
                                      message.Properties, message.ApplicationProperties, message.Body);
                    }
                }
                if (!options.Synchronous)
                {
                    while (callbackCount < sentCount)
                    {
                        await Task.Delay(1); // wait for dispositions to be delivered
                    }
                }
                stopwatch.Stop();
                LoggerInfo(string.Format("Exiting. msgsSent= {0}, Performance: {1}",
                    sentCount, MessagesPerSecond(sentCount, stopwatch.ElapsedMilliseconds)));

                sender.Close();
                session.Close();
                connection.Close();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Exception {0}.", e);
                if (null != connection)
                    connection.Close();
                exitCode = ERROR_OTHER;
            }
            return exitCode;
        }

        async Task WaitForCompletion(List<Task> taskList)
        {
            await Task.WhenAll(taskList.ToArray());
        }

        static public void Main(string[] args)
        {
            Options options = new Options(args);
            if (options.Instances == 0)
                return;

            List<Spout> spoutList = new List<Spout>();
            List<Task> taskList = new List<Task>();
            for (int idx = 0; idx < options.Instances; idx++)
            {
                var thisSpout = new Spout(options, idx);
                var thisTask = Task.Run( thisSpout.Run );
                spoutList.Add(thisSpout);
                taskList.Add(thisTask);
            }
            spoutList[0].WaitForCompletion(taskList).Wait();

            int tSent = 0;
            long tMs = 0;
            for (int idx = 0; idx < options.Instances; idx++)
            {
                tSent += spoutList[idx].sentCount;
                tMs += spoutList[idx].stopwatch.ElapsedMilliseconds;
            }
            tMs /= options.Instances;
            spoutList[0].LoggerInfo(string.Format("Async.Spout exit totals: " +
                "msgsOut: {0}, Performance: {1}",
                tSent, spoutList[0].MessagesPerSecond(tSent, tMs)));
        }
    }
}
