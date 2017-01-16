using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Threading;
using Amqp;
using Amqp.Framing;
using System.Transactions;

namespace Test.Amqp
{
    class Program
    {
        static void TransactedPosting(Address addr, string target)
        {
            string testName = "TransactedPosting";
            int nMsgs = 5;

            Connection connection = new Connection(addr);
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session, "sender-" + testName, target);

            // commit
            using (var ts = new TransactionScope())
            {
                for (int i = 0; i < nMsgs; i++)
                {
                    Message message = new Message("test");
                    message.Properties = new Properties() { MessageId = "commit" + i, GroupId = testName };
                    sender.Send(message);
                }

                ts.Complete();
            }

            // rollback
            using (var ts = new TransactionScope())
            {
                for (int i = nMsgs; i < nMsgs * 2; i++)
                {
                    Message message = new Message("test");
                    message.Properties = new Properties() { MessageId = "rollback" + i, GroupId = testName };
                    sender.Send(message);
                }
            }

            // commit
            using (var ts = new TransactionScope())
            {
                for (int i = 0; i < nMsgs; i++)
                {
                    Message message = new Message("test");
                    message.Properties = new Properties() { MessageId = "commit" + i, GroupId = testName };
                    sender.Send(message);
                }

                ts.Complete();
            }

            ReceiverLink receiver = new ReceiverLink(session, "receiver-" + testName, target);
            for (int i = 0; i < nMsgs * 2; i++)
            {
                Message message = receiver.Receive();
                Trace.WriteLine(TraceLevel.Information, "receive: {0}", message.Properties.MessageId);
                receiver.Accept(message);
                if (!message.Properties.MessageId.StartsWith("commit"))
                {
                    Console.Error.WriteLine("MessageId does not start with 'commit'");
                }
            }

            connection.Close();
        }

        static void TransactedRetiring(Address addr, string target)
        {
            string testName = "TransactedRetiring";
            int nMsgs = 10;

            Connection connection = new Connection(addr);
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session, "sender-" + testName, target);

            // send one extra for validation
            for (int i = 0; i < nMsgs + 1; i++)
            {
                Message message = new Message("test");
                message.Properties = new Properties() { MessageId = "msg" + i, GroupId = testName };
                sender.Send(message);
            }

            ReceiverLink receiver = new ReceiverLink(session, "receiver-" + testName, target);
            Message[] messages = new Message[nMsgs];
            for (int i = 0; i < nMsgs; i++)
            {
                messages[i] = receiver.Receive();
                Trace.WriteLine(TraceLevel.Information, "receive: {0}", messages[i].Properties.MessageId);
            }

            // commit harf
            using (var ts = new TransactionScope())
            {
                for (int i = 0; i < nMsgs / 2; i++)
                {
                    receiver.Accept(messages[i]);
                }

                ts.Complete();
            }

            // rollback
            using (var ts = new TransactionScope())
            {
                for (int i = nMsgs / 2; i < nMsgs; i++)
                {
                    receiver.Accept(messages[i]);
                }
            }

            // after rollback, messages should be still acquired
            {
                Message message = receiver.Receive();
                if (!message.Properties.MessageId.Equals("msg" + nMsgs))
                {
                    Console.Error.WriteLine("MessageId does not match sequence");
                }
                receiver.Release(message);
            }

            // commit
            using (var ts = new TransactionScope())
            {
                for (int i = nMsgs / 2; i < nMsgs; i++)
                {
                    receiver.Accept(messages[i]);
                }

                ts.Complete();
            }

            // only the last message is left
            {
                Message message = receiver.Receive();
                if (!message.Properties.MessageId.Equals("msg" + nMsgs))
                {
                    Console.Error.WriteLine("MessageId does not match sequence");
                }
                receiver.Accept(message);
            }

            // at this point, the queue should have zero messages.
            // If there are messages, it is a bug in the broker.

            connection.Close();
        }

        static void TransactedRetiringAndPosting(Address addr, string target)
        {
            string testName = "TransactedRetiringAndPosting";
            int nMsgs = 10;

            Connection connection = new Connection(addr);
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session, "sender-" + testName, target);

            for (int i = 0; i < nMsgs; i++)
            {
                Message message = new Message("test");
                message.Properties = new Properties() { MessageId = "msg" + i, GroupId = testName };
                sender.Send(message);
            }

            ReceiverLink receiver = new ReceiverLink(session, "receiver-" + testName, target);

            receiver.SetCredit(2, false);
            Message message1 = receiver.Receive();
            Message message2 = receiver.Receive();

            // ack message1 and send a new message in a txn
            using (var ts = new TransactionScope())
            {
                receiver.Accept(message1);

                Message message = new Message("test");
                message.Properties = new Properties() { MessageId = "msg" + nMsgs, GroupId = testName };
                sender.Send(message);

                ts.Complete();
            }

            // ack message2 and send a new message in a txn but abort the txn
            using (var ts = new TransactionScope())
            {
                receiver.Accept(message2);

                Message message = new Message("test");
                message.Properties = new Properties() { MessageId = "msg" + (nMsgs + 1), GroupId = testName };
                sender.Send(message1);
            }

            receiver.Release(message2);

            // receive all messages. should see the effect of the first txn
            receiver.SetCredit(nMsgs, false);
            for (int i = 1; i <= nMsgs; i++)
            {
                Message message = receiver.Receive();
                Trace.WriteLine(TraceLevel.Information, "receive: {0}", message.Properties.MessageId);
                receiver.Accept(message);
                if (!message.Properties.MessageId.Equals("msg" + i))
                {
                    Console.Error.WriteLine("MessageId does not match sequence");
                }
            }

            connection.Close();
        }



        static void Main(string[] args)
        {
            Address address = new Address("amqp://10.19.176.108:5672");
            string target = "q1";

            Connection.DisableServerCertValidation = true;
            // uncomment the following to write frame traces
            Trace.TraceLevel = TraceLevel.Frame;
            Trace.TraceListener = (f, a) => System.Diagnostics.Trace.WriteLine(DateTime.Now.ToString("[hh:mm:ss.fff]") + " " + string.Format(f, a));

            TransactedPosting(address, target);
            TransactedRetiring(address, target);
            TransactedRetiringAndPosting(address, target);

        }
    }
}

