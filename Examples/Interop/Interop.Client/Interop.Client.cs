//#define CLIENT_MANAGES_CREDIT
#define LITE_MANAGES_CREDIT

using System;
using System.Collections.Generic;
using System.Threading;
using Amqp.Framing;
//using Amqp.Extensions;
using Amqp.Sasl;
using Amqp.Types;
using System.Threading.Tasks;

namespace Amqp.Extensions.Examples
{
    class Program
    {
        static void Main(string[] args)
        {

            Random rnd = new Random();

            Connection connection = new Connection(new Address("amqp://10.3.116.84:5672"));
            Session session = new Session(connection);
            ReceiverLink receiver = new ReceiverLink(session, "receiver", "orders");

            // sleep to make sure the queue is created before we send, we could await the attach frame but this is just a demo.
            Thread.Sleep(1000);

            SenderLink sender = new SenderLink(session, "sender", "orders");
            Message message = new Message("a message!");
            message.Header = new Header();
            message.Header.Durable = true;

            Console.WriteLine("Sending N messages...");
            int N = 200;
            for (var i = 0; i < N; i++)
            {
                sender.Send(message);
            }
            Console.WriteLine(".... Done sending");

            Trace.TraceLevel = TraceLevel.Verbose | TraceLevel.Error |
            TraceLevel.Frame | TraceLevel.Information | TraceLevel.Warning;
            Trace.TraceListener = (l, f, o) => Console.WriteLine(DateTime.Now.ToString("[hh:mm:ss.fff]") + " " + string.Format(f, o));

            sender.Close();

#if CLIENT_MANAGES_CREDIT
            int nInProgress = 0;
            int msgsRetired = 0;
            const int creditEveryN = 5;
            const int credit = 10;
            int msgsRetired = 0;

            receiver.Start(0, async (r, m) =>
            {
                int depth;
                int nRetired;
                depth = Interlocked.Increment(ref nInProgress);
                double delay = System.Convert.ToDouble(rnd.Next(2, 20));
                Console.WriteLine("Depth= {0} In receive callback. Starting await of {1} seconds", depth, delay);
                await Task.Delay(TimeSpan.FromSeconds(delay));
                r.Accept(m);
                depth = Interlocked.Decrement(ref nInProgress);
                Console.WriteLine("Depth= {0} Exiting.  Accepting message", depth);
                nRetired = Interlocked.Increment(ref msgsRetired);
                if (nRetired % creditEveryN == 0)
                {
                    receiver.SetCredit(credit - depth, false);
                }
            });
            receiver.SetCredit(credit, false);
#endif

#if LITE_MANAGES_CREDIT
            // never ack... just delay so we can see messages "in-flight" for 60 seconds.
            // we should only see 10 received... we see all 1000 instead.
            int nIn = 0;
            int nDone = 0;
            receiver.Start(10, async (r, m) =>
            {
                nIn++;
                Console.WriteLine("nIn = {0}, nDone = {1}, InFlight = {2} In receive callback. Starting await...", nIn, nDone, nIn-nDone);
                double delay = System.Convert.ToDouble(rnd.Next(2, 20));
                await Task.Delay(TimeSpan.FromSeconds(delay));
                r.Accept(m);
                nDone++;
                Console.WriteLine("nIn = {0}, nDone = {1}, InFlight = {2} ... Exiting Task.Delay", nIn, nDone, nIn - nDone);
            });
#endif
            Thread.Sleep(3000000);
        }
    }
}
