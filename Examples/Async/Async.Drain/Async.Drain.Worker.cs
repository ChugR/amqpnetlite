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
using Amqp;
using System.Threading.Tasks;

namespace Examples.Async {
    /// <summary>
    /// Class that simulates message processing
    /// Drain instantiates an instance of this class and luanches
    /// it to execute "message processing work". It signals
    /// completion by calling the parent WorkerDone function.
    /// </summary>
    class Worker
    {
        private Drain parent;
        private int index;
        private Message msg;
        private double delay;

        public Worker(Drain parent_, int index_)
        {
            parent = parent_;
            index = index_;
        }

        /// <summary>
        /// Worker.Run This is where your application processes the
        /// message.
        /// </summary>
        async public Task Run()
        {
            await Task.Delay(TimeSpan.FromMilliseconds(delay));
            parent.WorkerDone(this, msg);
        }

        public int Index { get { return index; } }
        public Message Msg { set { msg = value; } }
        public double Delay
        {
            set { delay = value; }
            get { return delay; }
        }
    }

}
