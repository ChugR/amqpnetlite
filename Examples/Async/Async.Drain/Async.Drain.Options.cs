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

namespace Examples.Async
{
    using System;

    public class Options
    {
        private string address;
        private int    count;
        private int    creditInitial;
        private bool   creditManual;
        private int    creditRefresh;
        private bool   forever;
        private int    instances;
        private bool   logDebug;
        private bool   logTrace;
        private bool   quiet;
        //private bool   synchronous;
        private int    taskDelayMin;
        private int    taskDelayMax;
        private int    taskPoolSize;
        private int    testDuration;
        private string url;

        public Options(string[] args)
        {
            address       = "";
            count         = 1;
            creditInitial = 10;
            creditManual  = false;
            creditRefresh = 5;
            forever       = false;
            instances     = 1;
            logDebug      = false;
            logTrace      = false;
            quiet         = false;
            //synchronous   = false;
            taskDelayMin = 10;
            taskDelayMax = 100;
            taskPoolSize = 5;
            testDuration = 0;
            url           = "amqp://guest:guest@127.0.0.1:5672";
            Parse(args);
        }

        private void Usage()
        {
            Console.WriteLine("Usage: Async.Drain [OPTIONS] --address STRING");
            Console.WriteLine("");
            Console.WriteLine("Create a connection, attach a receiver to an address, receive and process messages.");
            Console.WriteLine("  Messages are queued and later disposed by a pool of worker tasks.");
            Console.WriteLine("  Process exits when 'count' messages have been accepted or task duration expires.");
            Console.WriteLine("  Multiple instances may be launched, each sharing same options.");
            Console.WriteLine("Options:");
            Console.WriteLine(" --address STRING        []      - AMQP 1.0 terminus name");
            Console.WriteLine(" --broker [amqp://guest:guest@127.0.0.1:5672] - AMQP 1.0 peer connection address");
            Console.WriteLine(" --count INT             [1]     - receive this many messages and exit; 0 disables count based exit");
            Console.WriteLine(" --credit-initial INT    [10]    - receiver initial credit");
            Console.WriteLine(" --credit-manual         [false] - disable Lite autoRefresh");
            Console.WriteLine(" --credit-refresh INT    [5]     - manually refresh credits every N messages");
            Console.WriteLine(" --forever               [false] - use infinite test duration");
            Console.WriteLine(" --instances INT         [1]     - run this many instances of Drain");
            Console.WriteLine(" --log-debug             [false] - print debug log to console");
            Console.WriteLine(" --log-trace             [false] - print trace log to console");
            Console.WriteLine(" --quiet                 [false] - do not print each message's content");
            //Console.WriteLine(" --synchronous           [false] - read messages synchronously vs. using async callbacks");
            Console.WriteLine(" --task-delay-min-ms INT [10]    - minimum processing delay");
            Console.WriteLine(" --task-delay-max-ms INT [100]   - maximum processing delay");
            Console.WriteLine(" --task-pool-size INT    [5]     - number of tasks processing messages");
            Console.WriteLine(" --test-duration SECONDS [0]     - run the test for this many seconds. 0 disables duration time limit.");
            Console.WriteLine(" --help                          - print this message and exit");
            Console.WriteLine("");
            Console.WriteLine("Exit codes:");
            Console.WriteLine(" 0 - successfully received all messages");
            Console.WriteLine(" 1 - timeout waiting for a message");
            Console.WriteLine(" 2 - other error");
            System.Environment.Exit(2);
        }

        private void Parse(string[] args)
        {
            int argCount = args.Length;
            int current = 0;

            while (current < argCount)
            {
                string arg = args[current];
                if (arg == "--help")
                {
                    Usage();
                }
                else if (arg == "--broker")
                {
                    this.url = args[++current];
                }
                else if (arg == "--address")
                {
                    this.address = args[++current];
                }
                else if (arg == "--test-duration")
                {
                    arg = args[++current];
                    int i = int.Parse(arg);
                    if (i >= 0)
                    {
                        this.testDuration = i;
                    }
                }
                else if (arg == "--forever")
                {
                    this.forever = true;
                }
                else if (arg == "--count")
                {
                    arg = args[++current];
                    int i = int.Parse(arg);
                    if (i >= 0)
                    {
                        this.count = i;
                    }
                }
                else if (arg == "--credit-initial")
                {
                    arg = args[++current];
                    int i = int.Parse(arg);
                    if (i >= 0)
                    {
                        this.creditInitial = i;
                    }
                }
                else if (arg == "--credit-manual")
                {
                    this.creditManual = true;
                }
                else if (arg == "--credit-refresh")
                {
                    arg = args[++current];
                    int i = int.Parse(arg);
                    if (i >= 0)
                    {
                        this.creditRefresh = i;
                    }
                }
                else if (arg == "--quiet")
                {
                    this.quiet = true;
                }
                else if (arg == "--instances")
                {
                    arg = args[++current];
                    int i = int.Parse(arg);
                    if (i >= 0)
                    {
                        this.instances = i;
                    }
                }
                else if (arg == "--log-debug")
                {
                    this.logDebug = true;
                }
                else if (arg == "--log-trace")
                {
                    this.logTrace = true;
                }
                else if (arg == "--task-delay-min-ms")
                {
                    arg = args[++current];
                    int i = int.Parse(arg);
                    if (i >= 0)
                    {
                        this.taskDelayMin = i;
                    }
                }
                else if (arg == "--task-delay-max-ms")
                {
                    arg = args[++current];
                    int i = int.Parse(arg);
                    if (i >= 0)
                    {
                        this.taskDelayMax = i;
                    }
                }
                else if (arg == "--task-pool-size")
                {
                    arg = args[++current];
                    int i = int.Parse(arg);
                    if (i >= 0)
                    {
                        this.taskPoolSize = i;
                    }
                }
                //else if (arg == "--synchronous")
                //{
                //    this.synchronous = true;
                //}
                else
                {
                    throw new ArgumentException(String.Format("unknown argument \"{0}\"", arg));
                }

                current++;
            }

            if (address.Equals(""))
            {
                throw new ArgumentException("missing argument: --address");
            }
            if (taskDelayMax.CompareTo(taskDelayMin) < 0)
            {
                throw new ArgumentException("Task delay max must be equal to or greater than task delay min");
            }
            if (testDuration > 0 && forever)
            {
                throw new ArgumentException("Args --forever and --task-duration conflict. Specify one or the other.");
            }
            if (creditManual)
            {
                throw new ArgumentException("Manual credit restoration is not yet implemented.");
            }
        }

        public string Url
        {
            get { return url; }
        }

        public string Address
        {
            get { return address; }
        }

        public int TestDuration
        {
            get { return testDuration; }
        }

        public bool Forever
        {
            get { return forever; }
        }

        public int Count
        {
            get { return count; }
        }

        public int CreditInitial
        {
            get { return creditInitial; }
        }

        public bool CreditManual
        {
            get { return creditManual; }
        }

        public int CreditReset
        {
            get { return creditRefresh; }
        }

        public int Instances
        {
            get { return instances; }
        }

        public bool LogDebug
        {
            get { return logDebug; }
        }

        public bool LogTrace
        {
            get { return logTrace; }
        }

        public int TaskDelayMin
        {
            get { return taskDelayMin; }
        }

        public int TaskDelayMax
        {
            get { return taskDelayMax; }
        }

        public int TaskPoolSize
        {
            get { return taskPoolSize; }
        }

        public bool Quiet
        {
            get { return quiet; }
        }
    }
}
