using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Threading;
using Amqp;
using Amqp.Framing;
using System.Transactions;

namespace TransactionTestProgram
{
    class Tests
    {
        public void log(string what)
        {
            System.Diagnostics.Trace.WriteLine(DateTime.Now.ToString("[hh:mm:ss.fff]") + " " + what);
            //Console.Error.WriteLine(DateTime.Now.ToString("[hh:mm:ss.fff]") + " " + what);
        }

        public void TransactedPosting(Address addr, string target)
        {
            string testName = "TransactedPosting";
            int nMsgs = 5;
            Boolean testpass = true;

            Connection connection = new Connection(addr);
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session, "sender-" + testName, target);

            // commit
            log(testName);
            log(testName + ": creating transaction scope for Complete set");
            log(testName);
            using (var ts = new TransactionScope())
            {
                for (int i = 0; i < nMsgs; i++)
                {
                    log("Writing message with id commit" + i);
                    Message message = new Message("test");
                    message.Properties = new Properties() { MessageId = "commit" + i, GroupId = testName };
                    sender.Send(message);
                }
                log("Transaction scope Complete()");
                ts.Complete();
            }

            // rollback
            log(testName + ": creating transaction scope for Rollback set");
            using (var ts = new TransactionScope())
            {
                for (int i = nMsgs; i < nMsgs * 2; i++)
                {
                    log("Writing message with id rollback" + i);
                    Message message = new Message("test");
                    message.Properties = new Properties() { MessageId = "rollback" + i, GroupId = testName };
                    sender.Send(message);
                }
                log("Exiting transaction scope without calling complete");
            }

            // commit
            log(testName + ": creating txn scope for 2nd Complete set");
            using (var ts = new TransactionScope())
            {
                for (int i = 0; i < nMsgs; i++)
                {
                    log("Writing message with id commit" + i);
                    Message message = new Message("test");
                    message.Properties = new Properties() { MessageId = "commit" + i, GroupId = testName };
                    sender.Send(message);
                }
                log("Transaction scope Complete()");
                ts.Complete();
            }

            log("Receiving two blocks of messages");
            ReceiverLink receiver = new ReceiverLink(session, "receiver-" + testName, target);
            for (int i = 0; i < nMsgs * 2; i++)
            {
                Message message = receiver.Receive();
                Trace.WriteLine(TraceLevel.Information, "receive: {0}", message.Properties.MessageId);
                receiver.Accept(message);
                if (!message.Properties.MessageId.StartsWith("commit"))
                {
                    Console.Error.WriteLine("MessageId does not start with 'commit'");
                    testpass = false;
                }
            }

            log(testName + " exiting with status " + (testpass ? "PASS" : "FAIL"));
            connection.Close();
        }

        public void TransactedRetiring(Address addr, string target)
        {
            string testName = "TransactedRetiring";
            int nMsgs = 10;
            bool testpass = true;

            Connection connection = new Connection(addr);
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session, "sender-" + testName, target);

            // send one extra for validation
            log(testName);
            log(testName + " Send N+1 with no transaction scope");
            log(testName);
            for (int i = 0; i < nMsgs + 1; i++)
            {
                log("Sending message with Id msg" + i);
                Message message = new Message("test");
                message.Properties = new Properties() { MessageId = "msg" + i, GroupId = testName };
                sender.Send(message);
            }

            ReceiverLink receiver = new ReceiverLink(session, "receiver-" + testName, target);
            Message[] messages = new Message[nMsgs];
            log("");
            log("Receive N messages but don't accept any");
            log("");
            for (int i = 0; i < nMsgs; i++)
            {
                messages[i] = receiver.Receive();
                Trace.WriteLine(TraceLevel.Information, "receive: {0}", messages[i].Properties.MessageId);
            }

            // commit harf
            log("Create txn scope and accept half the messages");
            using (var ts = new TransactionScope())
            {
                for (int i = 0; i < nMsgs / 2; i++)
                {
                    log("Accepting messageId: " + messages[i].Properties.MessageId);
                    receiver.Accept(messages[i]);
                }
                log("Txn scope complete");
                ts.Complete();
            }

            // rollback
            log("");
            log("Create txn scope and accept other half BUT");
            log(" do that in a failed txn scope that should roll back.");
            log("");
            using (var ts = new TransactionScope())
            {
                for (int i = nMsgs / 2; i < nMsgs; i++)
                {
                    log("Accepting messageId: " + messages[i].Properties.MessageId);
                    receiver.Accept(messages[i]);
                }
                log("Close txn scope without calling complete");
            }

            // after rollback, messages should be still acquired
            {
                log("Receiving a single message");
                Message message = receiver.Receive();
                if (!message.Properties.MessageId.Equals("msg" + nMsgs))
                {
                    log("MessageId: " + message.Properties.MessageId +
                        " does not match expected msg" + nMsgs);
                    testpass = false;
                }
                log("Releasing last message received, the extra one");
                receiver.Release(message);
            }

            // commit
            log("Creating txn scope to accept 2nd half for real this time");
            using (var ts = new TransactionScope())
            {
                for (int i = nMsgs / 2; i < nMsgs; i++)
                {
                    log("Accepting messageId " + messages[i].Properties.MessageId);
                    receiver.Accept(messages[i]);
                }
                log("Txn Complete()");
                ts.Complete();
            }

            // only the last message is left
            {
                log("Receive last message again");
                Message message = receiver.Receive();
                if (!message.Properties.MessageId.Equals("msg" + nMsgs))
                {
                    log("MessageId: " + message.Properties.MessageId +
                        " does not match expected msg" + nMsgs);
                    testpass = false;
                }
                log("Acccept last message");
                receiver.Accept(message);
            }

            // at this point, the queue should have zero messages.
            // If there are messages, it is a bug in the broker.
            log(testName + " exiting with status " + (testpass ? "PASS" : "FAIL"));

            connection.Close();
        }

        public void TransactedRetiringAndPosting(Address addr, string target)
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
    }

    class Program
    {
        static void Main(string[] args)
        {
            Address address = new Address("amqp://10.19.176.108:5672");
//            Address address = new Address("amqp://admin:password@10.10.56.155:5672");
            if (args.Length > 0)
            {
                address = new Address(args[0]);
            }
            string target = "q1";

            Connection.DisableServerCertValidation = true;
            // uncomment the following to write frame traces
            Trace.TraceLevel = TraceLevel.Frame | TraceLevel.Verbose | TraceLevel.Output;
            Trace.TraceListener = (f, a) => System.Diagnostics.Trace.WriteLine(DateTime.Now.ToString("[hh:mm:ss.fff]") + " " + string.Format(f, a));

            Tests tests = new TransactionTestProgram.Tests();

            tests.TransactedPosting(address, target);
            tests.TransactedRetiring(address, target);
            //TransactedRetiringAndPosting(address, target);

        }
    }
}

