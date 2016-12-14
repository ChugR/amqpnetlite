using System;
using System.Collections.Generic;
using System.Threading;
using System.Web.Script.Serialization;
using Amqp;
using Amqp.Framing;
using Amqp.Types;

namespace Qpidit
{
    class MessageValue
    {
    }

    class Sender
    {
        private string brokerUrl;
        private string queueName;
        private string amqpType;
        private string jsonMessages;

        public Sender(string brokerUrl_, string queueName_, string amqpType_, string jsonMessages_)
        {
            brokerUrl = brokerUrl_;
            queueName = queueName_;
            amqpType = amqpType_;
            jsonMessages = jsonMessages_;
        }

        ~Sender()
        { }


        public Message GenerateMessage(string amqpType, object messageObject)
        {
            return new Message();
        }


        public void run()
        {
            List<Message> messagesToSend = new List<Message>();

            // Deserialize the json message list
            JavaScriptSerializer jss = new JavaScriptSerializer();
            var itMsgs = jss.Deserialize<dynamic>(jsonMessages);
            if (!itMsgs is Array)
                throw new ApplicationException(
                    String.Format("messages are not formatted as a json list: {0}", jsonMessages));

            // Generate messages
            foreach (object itMsg in (Array)itMsgs)
            {
                messagesToSend.Add( GenerateMessage(amqpType, itMsg));
            }

            // Send the messages
            ManualResetEvent senderAttached = new ManualResetEvent(false);
            OnAttached onSenderAttached = (l, a) => { senderAttached.Set(); };
            Address address = new Address(string.Format("amqp://{0}", brokerUrl));
            Connection connection = new Connection(address);
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session,
                                               "Lite-amqp-types-test-sender",
                                               new Target() { Address = queueName },
                                               onSenderAttached);
            if (senderAttached.WaitOne(10000))
            {
                foreach (Message message in messagesToSend)
                {
                    sender.Send(message);
                }
            }
            else
            {
                throw new ApplicationException(string.Format(
                    "Time out attaching to test broker {0} queue {1}", brokerUrl, queueName));
            }

            sender.Close();
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
             *       4: Test value(s) as JSON string
             */
            if (args.Length != 4)
            {
                throw new System.ArgumentException(
                    "Required argument count must be 4: brokerAddr queueName amqpType jsonValuesToSend");
            }
            int exitCode = 0;

            //Trace.TraceLevel = TraceLevel.Frame | TraceLevel.Verbose;
            //Trace.TraceListener = (f, a) => Console.WriteLine(DateTime.Now.ToString("[hh:mm:ss.fff]") + " " + string.Format(f, a));

            try
            {
                Sender sender = new Qpidit.Sender(
                    args[0], args[1], args[2], args[3]);
                sender.run();
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
