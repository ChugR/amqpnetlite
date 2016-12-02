using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amqp;
using Amqp.Types;
using System.Runtime.Serialization.Json;

namespace Receiver
{
    class Receiver
    {
        static void Main(string[] args)
        {
            /*
             * --- main ---
             * Args: 1: Broker address (ip-addr:port)
             *       2: Queue name
             *       3: AMQP type
             *       4: Expected number of test values to receive
             */
            if (args.Length != 4)
            {
                throw new System.ArgumentException("Required argument count must be 4: brokerAddr queueName amqpType nValues");
            }

            foreach (string arg in args)
            {
                Console.WriteLine(arg);
            }

        }
    }
}
