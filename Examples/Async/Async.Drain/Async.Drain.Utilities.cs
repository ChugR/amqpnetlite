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
using System.Threading.Tasks;


namespace Examples.Async {

    /// <summary>
    /// Task extension to run a task without waiting for it to complete.
    /// OnException the task prints the exception to stderr.
    /// </summary>
    static class TaskExtensions
    {
        public static void Forget(this Task task)
        {
            task.
                ContinueWith(t => { Console.Error.WriteLine(t.Exception); },
                TaskContinuationOptions.OnlyOnFaulted);
        }
    }

    /// <summary>
    /// Async version of AutoResetEvent
    ///
    /// Credit to:
    /// Building Async Coordination Primitives, Part 2: AsyncAutoResetEvent
    /// February 11, 2012 by Stephen Toub - MSFT
    /// https://blogs.msdn.microsoft.com/pfxteam/2012/02/11/building-async-coordination-primitives-part-2-asyncautoresetevent/
    /// </summary>
    public class AsyncAutoResetEvent
    {
        private static readonly Task s_completed = Task.FromResult(true);
        private readonly Queue<TaskCompletionSource<bool>> m_waits = new Queue<TaskCompletionSource<bool>>();
        private bool m_signaled;

        public Task WaitAsync()
        {
            lock (m_waits)
            {
                if (m_signaled)
                {
                    m_signaled = false;
                    return s_completed;
                }
                else
                {
                    var tcs = new TaskCompletionSource<bool>();
                    m_waits.Enqueue(tcs);
                    return tcs.Task;
                }
            }
        }

        public void Set()
        {
            TaskCompletionSource<bool> toRelease = null;

            lock (m_waits)
            {
                if (m_waits.Count > 0)
                    toRelease = m_waits.Dequeue();
                else if (!m_signaled)
                    m_signaled = true;
            }

            toRelease?.SetResult(true);
        }
    }

    public static class Helpers
    {
        /// <summary>
        /// Compute and format elapsed time and messages per second.
        /// </summary>
        public static string MessagesPerSecond(int totalMessages, long elapsedMS)
        {
            double nMsgs = Convert.ToDouble(totalMessages);
            double emS = Convert.ToDouble(elapsedMS);
            if (emS == 0.0) emS = 1.0;
            double secs =  emS / 1000.0;
            string result = string.Format("time(S): {0:N3}, msg/S: {1:N2}",
                secs, nMsgs / emS * 1000.0);
            return result;
        }
    }
}
