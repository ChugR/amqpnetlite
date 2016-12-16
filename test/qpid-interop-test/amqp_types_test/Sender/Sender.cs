using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Web.Script.Serialization;
using Amqp;
using Amqp.Framing;
using Amqp.Types;

namespace Qpidit
{
    /// <summary>
    /// MessageValue holds a QpidIT type name and a json object created from the
    /// CLI string argument, encodes it, and returns the object to be used as the
    /// constructor for a message to be sent.
    /// Complex values List and Map are constructed recursively.
    /// Remaining singleton values like int or long are held directly as objects.
    /// </summary>
    class MessageValue
    {
        // Original type and json object
        private string baseType;
        private object baseValue;
        private Boolean encoded;

        // simple objects completely encoded
        private object valueDirect;

        // Lists
        private List<MessageValue> valueList;

        // Maps
        // Kept as lists to avoid dictionary reordering complications.
        private List<MessageValue> valueMapKeys;
        private List<MessageValue> valueMapValues;
        
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="type">qpidit type name</param>
        /// <param name="value">json encoded object</param>
        public MessageValue(string type, object value)
        {
            baseType = type;
            baseValue = value;
            encoded = false;
            valueDirect = null;
            valueList = null;
            valueMapKeys = null;
            valueMapValues = null;

        }


        public Message ToMessage()
        {
            if (!encoded)
                Encode();

            if (valueDirect != null)
            {
                Message m = new Message(valueDirect);
                return m;
            }
            throw new ApplicationException("Message not encoded");
        }


        public string StripLeading0x(string value)
        {
            if (!value.StartsWith("0x"))
                throw new ApplicationException(String.Format(
                    "EncodeUInt string does not start with '0x' : {0}", value));
            return value.Substring(2);
        }

        public UInt64 EncodeUInt(string value)
        {
            UInt64 result = 0;
            value = StripLeading0x(value);
            result = UInt64.Parse(value, System.Globalization.NumberStyles.HexNumber);
            return result;
        }


        public Int64 EncodeInt(string value)
        {
            Int64 result = 0;
            bool isNegated = value.StartsWith("-");
            if (isNegated)
                value = value.Substring(1);
            value = StripLeading0x(value);
            result = Int64.Parse(value, System.Globalization.NumberStyles.HexNumber);
            if (isNegated)
                result = -result;
            return result;
        }


        /// <summary>
        /// Gnerate the object used to create a message
        /// </summary>
        public void Encode()
        {
            string value;
            UInt64 valueUInt64;
            Int64 valueInt64;

            switch (baseType)
            {
                case "boolean":
                    value = (string)baseValue;
                    bool mybool = String.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
                    valueDirect = mybool;
                    break;
                case "ubyte":
                    value = (string)baseValue;
                    valueUInt64 = EncodeUInt(value);
                    Byte mybyte = (Byte)(valueUInt64 & 0xff);
                    valueDirect = mybyte;
                    break;
                case "ushort":
                    value = (string)baseValue;
                    valueUInt64 = EncodeUInt(value);
                    UInt16 myuint16 = (UInt16)(valueUInt64 & 0xffff);
                    valueDirect = myuint16;
                    break;
                case "uint":
                    value = (string)baseValue;
                    valueUInt64 = EncodeUInt(value);
                    UInt32 myuint32 = (UInt32)(valueUInt64 & 0xffffffff);
                    valueDirect = myuint32;
                    break;
                case "ulong":
                    value = (string)baseValue;
                    valueUInt64 = EncodeUInt(value);
                    valueDirect = valueUInt64;
                    break;
                case "byte":
                    value = (string)baseValue;
                    valueInt64 = EncodeInt(value);
                    SByte mysbyte = (SByte)(valueInt64 & 0xff);
                    valueDirect = mysbyte;
                    break;
                case "short":
                    value = (string)baseValue;
                    valueInt64 = EncodeInt(value);
                    Int16 myint16 = (Int16)(valueInt64 & 0xffff);
                    valueDirect = myint16;
                    break;
                case "int":
                    value = (string)baseValue;
                    valueInt64 = EncodeInt(value);
                    Int32 myint32 = (Int32)(valueInt64 & 0xffffffff);
                    valueDirect = myint32;
                    break;
                case "long":
                    value = (string)baseValue;
                    valueInt64 = EncodeInt(value);
                    valueDirect = valueInt64;
                    break;
                case "float":
                    value = StripLeading0x((string)baseValue);
                    UInt32 num32 = UInt32.Parse(value, System.Globalization.NumberStyles.AllowHexSpecifier);
                    byte[] floatVals = BitConverter.GetBytes(num32);
                    float flt = BitConverter.ToSingle(floatVals, 0);
                    valueDirect = flt;
                    break;
                case "double":
                    value = StripLeading0x((string)baseValue);
                    UInt64 num64 = UInt64.Parse(value, System.Globalization.NumberStyles.AllowHexSpecifier);
                    byte[] doubleVals = BitConverter.GetBytes(num64);
                    double dbl = BitConverter.ToDouble (doubleVals, 0);
                    valueDirect = dbl;
                    break;
                case "timestamp":
                    // epochTicks is the number of 100uSec ticks between 01/01/0001
                    // and 01/01/1970. Used to adjust between DateTime and unix epoch.
                    const long epochTicks = 621355968000000000;
                    value = StripLeading0x((string)baseValue);
                    Int64 dtticks = Int64.Parse(value, System.Globalization.NumberStyles.AllowHexSpecifier);
                    dtticks *= TimeSpan.TicksPerMillisecond;
                    dtticks += epochTicks;
                    DateTime dt = new DateTime(dtticks, DateTimeKind.Utc);
                    valueDirect = dt;
                    break;
                case "uuid":
                    value = (string)baseValue;
                    Guid guid = new Guid(value);
                    valueDirect = guid;
                    break;
                case "binary":
                    // TODO: fix this
                    value = (string)baseValue;
                    byte[] binval = Encoding.ASCII.GetBytes(value);
                    valueDirect = binval;
                    break;
                case "string":
                    valueDirect = (string)baseValue;
                    break;
                case "symbol":
                    Symbol sym = new Symbol((string)baseValue);
                    valueDirect = sym;
                    break;
                case "list":
                    break;
                case "map":
                    break;
                default:
                    throw new ApplicationException(String.Format(
                        "Sender can not encode base type: {0}", baseType));
            }
            encoded = true;
        }
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

        /// <summary>
        /// Generate a message 
        /// </summary>
        /// <param name="amqpType">QpidIt data item type</param>
        /// <param name="messageObject">Deserialized json message object</param>
        /// <returns></returns>
        public Message GenerateMessage(string amqpType, object messageObject)
        {
            Message message = null;
            if (messageObject is Array)
            {
                throw new ApplicationException("Lists are TBD");
                //int entry = 0;
                //Console.WriteLine("{0} obj is array with {1} entries", level, ((Array)obj).Length);
                //foreach (object subobj in (Array)messageObject)
                //{
                //    Console.WriteLine("AS{0} entry {1}", level + 1, entry++);
                //    decodeThis(subobj, level);
                //}
            }
            else if (messageObject is Dictionary<string, object>)
            {
                throw new ApplicationException("Maps are TBD");
                //Dictionary<string, object> myDict = new Dictionary<string, object>();
                //myDict = (Dictionary<string, object>)messageObject;
                //Console.WriteLine("{0} obj is dictionary with {1} entries", level, myDict.Count);
                //int entry = 0;
                //foreach (var key in myDict.Keys)
                //{
                //    Console.WriteLine("{0} entry {1} key = {2}, value = ", level + 1, entry++, key);
                //    decodeThis(myDict[key], level);
                //}
            }
            else if (messageObject is String)
            {
                MessageValue mv = new MessageValue(amqpType, messageObject);
                mv.Encode();
                message = mv.ToMessage();
            }
            else
            {
                throw new ApplicationException("Could not decode message object");
            }
            return message;
        }


        public void run()
        {
            List<Message> messagesToSend = new List<Message>();

            // Deserialize the json message list
            JavaScriptSerializer jss = new JavaScriptSerializer();
            var itMsgs = jss.Deserialize<dynamic>(jsonMessages);
            if (!(itMsgs is Array))
                throw new ApplicationException(String.Format(
                    "Messages are not formatted as a json list: {0}", jsonMessages));

            // Generate messages
            foreach (object itMsg in (Array)itMsgs)
            {
                messagesToSend.Add( GenerateMessage(amqpType, itMsg) );
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
                Sender sender = new Qpidit.Sender(args[0], args[1], args[2], args[3]);
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
