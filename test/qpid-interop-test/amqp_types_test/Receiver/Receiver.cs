using System;
using System.Collections.Generic;
//using System.Linq;
//using System.Text;
using System.Threading;
using Amqp;
using Amqp.Framing;
using Amqp.Types;
//using System.Runtime.Serialization.Json;
using Newtonsoft.Json;


namespace Qpidit
{
    class Receiver
    {
        private string _brokerUrl;
        private string _queueName;
        private string _amqpType;
        private UInt32 _expected;
        private UInt32 _received;
        private string _receivedValueList;

        public Receiver(string brokerUrl, string queueName, string amqpType, UInt32 expected)
        {
            _brokerUrl = brokerUrl;
            _queueName = queueName;
            _amqpType = amqpType;
            _expected = expected;
            _received = 0;
            _receivedValueList = "";
        }

        ~Receiver()
        { }

        public string receivedValueList
        {
            get { return _receivedValueList; }
        }

        public void messageCallback(ReceiverLink receiver, Message message)
        {
            // Received a message
        }

        public void run()
        {
            ManualResetEvent receiverAttached = new ManualResetEvent(false);
            OnAttached onReceiverAttached = (l, a) =>
            {
                receiverAttached.Set();
            };

            Address address = new Address(string.Format("amqp://{0}", _brokerUrl));
            Connection connection = new Connection(address);
            Session session = new Session(connection);
            ReceiverLink receiverlink = new ReceiverLink(session,
                                                         "Lite-amqp-types-test-receiver",
                                                         new Source() { Address = _queueName },
                                                         onReceiverAttached);
            if (receiverAttached.WaitOne(10000))
            {
                while (_received < _expected)
                {
                    Message message = receiverlink.Receive(10000);
                    if (message != null)
                    {
                        // got one
                        _received += 1;
                        receiverlink.Accept(message);
                        _receivedValueList = string.Format("Received {0} of {1} messages", _received, _expected);
                    }
                    else
                    {
                        throw new ApplicationException(string.Format("Time out receiving message {0} of {1}", _received+1, _expected));
                    }
                }
            }
            else
            {
                throw new ApplicationException("Time out attaching to test queue");
            }

            receiverlink.Close();
            session.Close();
            connection.Close();
        }
    }

    class MainProgram
    {
        static int Main(string[] args)
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

            int exitCode = 0;

            foreach (string arg in args)
            {
                Console.WriteLine(arg);
            }

            try
            {
                //HACK: serialize objects into string
                //string instring = "[\"0x0\", \"0x7fffffff\", \"0x80000000\", \"0xffffffff\"]";
                List<int> listOfInts = new List<int>();
                listOfInts.Add(0);
                listOfInts.Add(1);
                listOfInts.Add(2);
                listOfInts.Add(-1);

                var result = JsonConvert.SerializeObject(listOfInts);
                Console.WriteLine("The list became: {0}", result);
                // End HACK Alert

                // create and run a receiver
                Receiver receiver = new Qpidit.Receiver(args[0], args[1], args[2], UInt32.Parse(args[3]));
                receiver.run();

                // Report result
                Console.WriteLine(args[2]);
                Console.WriteLine("{0}", receiver.receivedValueList);

            }
            catch (Exception e)
            {
                Console.Error.WriteLine("AmqpReceiver error: {0}.", e);
                exitCode = 1;
            }

            return exitCode;
        }
    }
}
