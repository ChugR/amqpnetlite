using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Amqp;
using Amqp.Framing;
using Amqp.Types;
//using System.Runtime.Serialization.Json;
//using Newtonsoft.Json;


namespace Qpidit
{
    /// <summary>
    /// Class to convert incoming Message into QPIDIT particulars
    /// </summary>
    class AnalyzedMessage
    {
        private Message _message;
        private string _liteType;
        private string _qType;
        private string _qString;


        public override string ToString()
        {
            return _qString;
        }


        /// <summary>
        /// Compute contents of byte array in reverse order.
        /// </summary>
        /// <param name="input">The byte array.</param>
        /// <param name="suppressLeading0s">Flag controls suppression of leading zeros.</param>
        /// <returns>Hexadecimal string</returns>
        public string BytesReversedToString(byte[] input, bool suppressLeading0s = false)
        {
            string result = "";
            for (int i = input.Length -1; i >= 0; i--)
            {
                if (! suppressLeading0s || input[i] != 0)
                {
                    suppressLeading0s = false;
                    result += String.Format("{0:x2}", input[i]);
                }
            }
            return result;
        }


        /// <summary>
        /// Return the input string surrounded with double quotes
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public string Quoted(string input)
        {
            return "\"" + input + "\"";
        }


        /// <summary>
        /// Constructor that analyzes the message
        /// </summary>
        /// <param name="message"></param>
        public AnalyzedMessage(Message message)
        {
            _message = message;

            object body = message.Body;
            if (body == null)
            {
                //Console.WriteLine("AnalyzedMessage body type : null");
                _liteType = "null";
                _qType = "null";
                _qString = Quoted("null");
            }
            else
            {
                _liteType = body.GetType().Name;
                //Console.WriteLine("AnalyzedMessage body type : {0}", _liteType);
                switch (_liteType) {
                    case "Boolean":
                        _qType = "boolean";
                        _qString = Quoted(((Boolean)body ? "True" : "False"));
                        break;
                    case "Byte":
                        _qType = "ubyte";
                        _qString = Quoted(String.Format("0x{0:x}", (Byte)body));
                        break;
                    case "UInt16":
                        _qType = "ushort";
                        _qString = Quoted(String.Format("0x{0:x}", (UInt16)body));
                        break;
                    case "UInt32":
                        _qType = "uint";
                        _qString = Quoted(String.Format("0x{0:x}", (UInt32)body));
                        break;
                    case "UInt64":
                        _qType = "ulong";
                        _qString = Quoted(String.Format("0x{0:x}", (UInt64)body));
                        break;
                    case "SByte":
                        _qType = "byte";
                        _qString = Quoted(String.Format("0x{0:x}", (SByte)body));
                        break;
                    case "Int16":
                        _qType = "short";
                        _qString = Quoted(String.Format("0x{0:x}", (Int16)body));
                        break;
                    case "Int32":
                        _qType = "int";
                        _qString = Quoted(String.Format("0x{0:x}", (Int32)body));
                        break;
                    case "Int64":
                        _qType = "long";
                        _qString = Quoted(String.Format("0x{0:x}", (Int64)body));
                        break;
                    case "Single":
                        byte[] sbytes = BitConverter.GetBytes((Single)body);
                        _qType = "float";
                        _qString = Quoted("0x" + BytesReversedToString(sbytes));
                        break;
                    case "Double":
                        byte[] dbytes = BitConverter.GetBytes((Double)body);
                        _qType = "double";
                        _qString = "\"0x" + BytesReversedToString(dbytes) + "\"";
                        break;
                    case "DateTime":
                        // epochTicks is the number of 100uSec ticks between 01/01/0001
                        // and 01/01/1970. Used to adjust between DateTime and unix epoch.
                        const long epochTicks = 621355968000000000;
                        byte[] dtbytes = BitConverter.GetBytes(
                            (((DateTime)body).Ticks - epochTicks) / TimeSpan.TicksPerMillisecond);
                        _qType = "timestamp";
                        _qString = Quoted("0x" + BytesReversedToString(dtbytes, true));
                        break;
                    case "Guid":
                        _qType = "uuid";
                        _qString = Quoted(body.ToString());
                        break;
                    case "Byte[]":
                        _qType = "binary";
                        byte[] binstr = (byte[])body;
                        StringBuilder builder = new StringBuilder();
                        foreach (byte b in binstr)
                            if (b >= 32 && b <= 127)
                                builder.Append((char)b);
                            else
                                builder.Append(String.Format("\\{0:x2}", b));
                        _qString = Quoted(builder.ToString());
                        break;
                    case "String":
                        _qType = "string";
                        _qString = Quoted(body.ToString());
                        break;
                    case "Symbol":
                        _qType = "symbol";
                        _qString = Quoted(body.ToString());
                        break;
                    case "List":
                        _qType = "list";
                        List list = (List)body;
                        string result = "[";
                        int li = 0;
                        foreach (string item in list)
                        {
                            if (li++ > 0) result += ", ";
                            result += Quoted(item);
                        }
                        result += "]";
                        _qString = result;
                        break;
                    case "Map":
                        _qType = "map";
                        Map map = (Map)body;
                        string mresult = "{";
                        int mi = 0;
                        foreach (var key in map.Keys)
                        {
                            if (mi++ > 0) mresult += ", ";
                            mresult += Quoted(key.ToString());
                            mresult += ":";
                            mresult += Quoted(map[key].ToString());
                        }
                        mresult += "}";
                        _qString = mresult;
                        break;
                    default:
                        _qType = "unknown";
                        _qString = String.Format("Unknown AMQP type: {0}", _liteType);
                        throw new ApplicationException(_qString);
                }
            }
        }

        public string LiteType {  get { return _liteType; } }
        public string QpiditType {  get { return _qType; } }
    }


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

        public string ReceivedValueList
        {
            get { return _receivedValueList; }
        }


        public void run()
        {
            ManualResetEvent receiverAttached = new ManualResetEvent(false);
            OnAttached onReceiverAttached = (l, a) => { receiverAttached.Set(); };
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
                        _received += 1;
                         receiverlink.Accept(message);
                        AnalyzedMessage am = new AnalyzedMessage(message);
                        if (am.QpiditType != _amqpType)
                        {
                            throw new ApplicationException(string.Format
                                ("Incorrect AMQP type found in message body: expected: {0}; found: {1}",
                                _amqpType, am.QpiditType));
                        }
                        // Console.WriteLine("{0} [{1}]", am.QpiditType, am);
                        if (_received > 1) _receivedValueList += ",";
                        _receivedValueList += am;
                    }
                    else
                    {
                        throw new ApplicationException(string.Format(
                            "Time out receiving message {0} of {1}", _received+1, _expected));
                    }
                }
            }
            else
            {
                throw new ApplicationException(string.Format(
                    "Time out attaching to test queue {0}", _queueName));
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
             *       3: QPIDIT AMQP type name of expected message body values
             *       4: Expected number of test values to receive
             */
            if (args.Length != 4)
            {
                throw new System.ArgumentException(
                    "Required argument count must be 4: brokerAddr queueName amqpType nValues");
            }
            int exitCode = 0;
            try
            {
                Receiver receiver = new Qpidit.Receiver(
                    args[0], args[1], args[2], UInt32.Parse(args[3]));
                receiver.run();

                Console.WriteLine(args[2]);
                Console.WriteLine("[{0}]", receiver.ReceivedValueList);
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
