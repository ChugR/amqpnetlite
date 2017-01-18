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
        public void log(string what, bool optionalPadding = false)
        {
            if (optionalPadding)
                System.Console.WriteLine("");
            System.Console.WriteLine(what);
            if (optionalPadding)
                System.Console.WriteLine("");
        }


        /// <summary>
        /// Test if queue is empty; if not then drain it.
        /// Uses 1 second timeout.
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="target"></param>
        /// <returns>True if any messages drained else False</returns>
        public bool DrainTarget(Address addr, string target)
        {
            bool result = false;
            Connection connection = new Connection(addr);
            Session session = new Session(connection);
            ReceiverLink rcvr = new ReceiverLink(session, "DrainTarget", target);

            Message leftover = rcvr.Receive(1000);
            while (leftover != null)
            {
                log("DIAG: Drained leftover message with Id: " + leftover.Properties.MessageId);
                rcvr.Accept(leftover);
                leftover = rcvr.Receive(1000);
                result = true;
            }
            rcvr.Close();
            session.Close();
            connection.Close();
            return result;
        }


        /// <summary>
        /// TransactedPosting
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="target"></param>
        public void TransactedPosting(Address addr, string target)
        {
            string testName = "TransactedPosting";
            int nMsgs = 5;
            Boolean testpass = true;

            DrainTarget(addr, target); 

            Connection connection = new Connection(addr);
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session, "sender-" + testName, target);

            // commit
            log(testName, true);
            log("nMsgs= " + nMsgs);
            log("Creating transaction scope");
            using (var ts = new TransactionScope())
            {
                for (int i = 0; i < nMsgs; i++)
                {
                    log("Writing message with id commit" + i);
                    Message message = new Message("test");
                    message.Properties = new Properties() { MessageId = "commit" + i, GroupId = testName };
                    sender.Send(message);
                }
                log("Calling scope Complete()");
                ts.Complete();
            }

            // rollback
            log("Creating transaction scope");
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
            log("Creating transaction scope");
            using (var ts = new TransactionScope())
            {
                for (int i = 0; i < nMsgs; i++)
                {
                    log("Writing message with id commit" + i);
                    Message message = new Message("test");
                    message.Properties = new Properties() { MessageId = "commit" + i, GroupId = testName };
                    sender.Send(message);
                }
                log("Calling scope Complete()");
                ts.Complete();
            }

            log("Receiving messages that should have been accepted under Txn scope");
            ReceiverLink receiver = new ReceiverLink(session, "receiver-" + testName, target);
            for (int i = 0; i < nMsgs * 2; i++)
            {
                Message message = receiver.Receive();
                Trace.WriteLine(TraceLevel.Information, "receive: {0}", message.Properties.MessageId);
                receiver.Accept(message);
                if (!message.Properties.MessageId.StartsWith("commit"))
                {
                    log("MessageId does not start with 'commit' : " + message.Properties.MessageId);
                    testpass = false;
                }
            }
            connection.Close();

            if (DrainTarget(addr, target))
            {
                log("Messages left in broker at end of test.");
                testpass = false;
            }

            log(testName + " exiting with status " + (testpass ? "PASS" : "FAIL"));
        }

        public void TransactedRetiring(Address addr, string target)
        {
            string testName = "TransactedRetiring";
            int nMsgs = 10;
            bool testpass = true;

            log("Test: " + testName, true);
            log("nMsgs= " + nMsgs);

            log("Pretest - draining target queue...", true);
            DrainTarget(addr, target);

            Connection connection = new Connection(addr);
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session, "sender-" + testName, target);

            // send one extra for validation
            log("Send N+1 with no transaction scope", true);
            for (int i = 0; i < nMsgs + 1; i++)
            {
                log("Sending message with Id msg" + i);
                Message message = new Message("test");
                message.Properties = new Properties() { MessageId = "msg" + i, GroupId = testName };
                sender.Send(message);
            }

            ReceiverLink receiver = new ReceiverLink(session, "receiver-" + testName, target);
            Message[] messages = new Message[nMsgs];
            log("Receive N messages but don't accept any", true);
            for (int i = 0; i < nMsgs; i++)
            {
                messages[i] = receiver.Receive();
                log("Received: " + messages[i].Properties.MessageId);
            }

            // commit half
            log("Create txn scope and accept half the messages", true);
            using (var ts = new TransactionScope())
            {
                for (int i = 0; i < nMsgs / 2; i++)
                {
                    log("Accepting to-be-committed messageId: " + messages[i].Properties.MessageId);
                    receiver.Accept(messages[i]);
                }
                log("Txn scope complete");
                ts.Complete();
            }

            // rollback
            log("Create txn scope and accept other half BUT do that in a failed txn scope that should roll back.", true);
            using (var ts = new TransactionScope())
            {
                for (int i = nMsgs / 2; i < nMsgs; i++)
                {
                    log("Accepting to-be-rolled-back messageId: " + messages[i].Properties.MessageId);
                    receiver.Accept(messages[i]);
                }
                log("Close txn scope without calling complete");
            }

            log("after rollback, messages should be still acquired", true);
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
            log("Creating txn scope to accept 2nd half for real this time", true);
            using (var ts = new TransactionScope())
            {
                for (int i = nMsgs / 2; i < nMsgs; i++)
                {
                    log("Accepting to-be-committed messageId " + messages[i].Properties.MessageId);
                    receiver.Accept(messages[i]);
                }
                log("Txn scope Complete()");
                ts.Complete();
            }

            // only the 'extra' message is left
            {
                log("Receive last message again", true);
                Message message = receiver.Receive();
                if (!message.Properties.MessageId.Equals("msg" + nMsgs))
                {
                    log("ERROR: MessageId: " + message.Properties.MessageId +
                        " does not match expected msg" + nMsgs);
                    testpass = false;
                }
                else
                {
                    log("Acccept last message");
                    receiver.Accept(message);
                }
            }
            receiver.Close();
            sender.Close();
            session.Close();
            connection.Close();

            // at this point, the queue should have zero messages.
            // If there are messages, it is a bug in the broker.
            // Try draining the queue and reporting the
            // message ids of the stuff left over.
            if (DrainTarget(addr, target))
            {
                log("ERROR: Messages left in broker at end of test.");
                testpass = false;
            }

            log(testName + " exiting with status " + (testpass ? "PASS" : "FAIL"));

        }

        public void TransactedRetiringAndPosting(Address addr, string target)
        {
            string testName = "TransactedRetiringAndPosting";
            int nMsgs = 10;

            DrainTarget(addr, target);

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
            // some laptop running ER14. User:admin, password:password, queue:q1
            Address address = new Address("amqp://admin:password@10.10.59.93:5672");

            if (args.Length > 0)
            {
                address = new Address(args[0]);
            }
            string target = "q1";
            if (args.Length > 1)
            {
                target = args[1];
            }

            Connection.DisableServerCertValidation = true;
            Trace.TraceLevel = TraceLevel.Frame | TraceLevel.Verbose | TraceLevel.Output;
            Trace.TraceListener = (f, a) => System.Console.WriteLine(DateTime.Now.ToString("[hh:mm:ss.fff]") + " " + string.Format(f, a));

            Tests tests = new TransactionTestProgram.Tests();

            // This works so skip for now: 
            // tests.TransactedPosting(address, target);

            tests.TransactedRetiring(address, target);

            // Haven't gotten here yet: 
            // tests.TransactedRetiringAndPosting(address, target);

        }
    }
}

