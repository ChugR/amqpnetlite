// Generates a D-Team table.
//
// These are the types To Be Tested
// 1. A type of 'System.Type' is encoded into which 0xNN encoding
// 2. A message encoded with 0xNN encoding becomes what 'System.Type'.
//
// null 	            0x40 	the null 
// boolean:true         0x41 	the boolean  true
// boolean:false        0x42 	the boolean  false
// uint:uint0           0x43 	the uint  0
// ulong:ulong0         0x44 	the ulong  0
// list:list0           0x45 	the empty list (i.e. the list with no elements)
// ubyte 	            0x50 	8-bit unsigned integer
// byte 	            0x51 	8-bit two's-complement integer
// uint:smallint        0x52 	unsigned integer  in the range 0 to 255 inclusive
// ulong:smallulong     0x53 	unsigned long  in the range 0 to 255 inclusive
// int:smallint         0x54 	signed integer  in the range -128 to 127 inclusive
// long:smalllong       0x55 	signed long  in the range -128 to 127 inclusive
// boolean 	            0x56 	boolean with the octet 0x00 being false and octet 0x01 being true
// ushort 	            0x60 	16-bit unsigned integer in network byte order
// short 	            0x61 	16-bit two's-complement integer in network byte order
// uint 	            0x70 	32-bit unsigned integer in network byte order
// int 	                0x71 	32-bit two's-complement integer in network byte order
// float:ieee-754 	    0x72 	IEEE 754-2008 binary32
// char:utf32           0x73	a UTF-32BE encoded unicode character
// decimal32:ieee-754 	0x74 	IEEE 754-2008 decimal32 using the Binary Integer Decimal encoding
// ulong 	            0x80 	64-bit unsigned integer in network byte order
// long 	            0x81 	64-bit two's-complement integer in network byte order
// double:ieee-754 	    0x82 	IEEE 754-2008 binary64
// timestamp:ms64       0x83    64-bit signed integer representing milliseconds since the unix epoch
// decimal64:754 	    0x84 	IEEE 754-2008 decimal64 using the Binary Integer Decimal encoding
// decimal128:754 	    0x94 	IEEE 754-2008 decimal128 using the Binary Integer Decimal encoding
// uuid 	            0x98 	UUID as defined in section 4.1.2 of RFC-4122
// binary:vbin8 	    0xa0 	up to 2^8 - 1 octets of binary data
// string:str8-utf8 	0xa1 	up to 2^8 - 1 octets worth of UTF-8 unicode (with no byte order mark)
// symbol:sym8 	        0xa3 	up to 2^8 - 1 seven bit ASCII characters representing a symbolic 
// binary:vbin32 	    0xb0 	up to 2^32 - 1 octets of binary data
// string:str32-utf8 	0xb1 	up to 2^32 - 1 octets worth of UTF-8 unicode (with no byte order mark)
// symbol:sym32 	    0xb3 	up to 2^32 - 1 seven bit ASCII characters representing a symbolic 
// list:list8 	        0xc0 	up to 2^8 - 1 list elements with total size less than 2^8 octets
// map:map8 	        0xc1 	up to 2^8 - 1 octets of encoded map data
// list:list32 	        0xd0 	up to 2^32 - 1 list elements with total size less than 2^32 octets
// map:map32 	        0xd1 	up to 2^32 - 1 octets of encoded map data
// array:array8 	    0xe0 	up to 2^8 - 1 array elements with total size less than 2^8 octets
// array:array32 	    0xf0 	up to 2^32 - 1 array elements with total size less than 2^32 octets
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using System.Reflection;

namespace type_table_gen
{
    public class AmqpTypeNames
    {
        Dictionary<byte, string> map = new Dictionary<byte, string>();
        public AmqpTypeNames()
        {
            map[(byte)0x40] = "null";
            map[(byte)0x41] = "boolean:true";
            map[(byte)0x42] = "boolean:false";
            map[(byte)0x43] = "uint:uint0";
            map[(byte)0x44] = "ulong:ulong0";
            map[(byte)0x45] = "list:list0";
            map[(byte)0x50] = "ubyte";
            map[(byte)0x51] = "byte";
            map[(byte)0x52] = "uint:smallint";
            map[(byte)0x53] = "ulong:smallulong";
            map[(byte)0x54] = "int:smallint";
            map[(byte)0x55] = "long:smalllong";
            map[(byte)0x56] = "boolean";
            map[(byte)0x60] = "ushort";
            map[(byte)0x61] = "short";
            map[(byte)0x70] = "uint";
            map[(byte)0x71] = "int";
            map[(byte)0x72] = "float:ieee-754";
            map[(byte)0x73] = "char:utf32";
            map[(byte)0x74] = "decimal32:ieee-754";
            map[(byte)0x80] = "ulong";
            map[(byte)0x81] = "long";
            map[(byte)0x82] = "double:ieee-754";
            map[(byte)0x83] = "timestamp:ms64";
            map[(byte)0x84] = "decimal64:754";
            map[(byte)0x94] = "decimal128:754";
            map[(byte)0x98] = "uuid";
            map[(byte)0xa0] = "binary:vbin8";
            map[(byte)0xa1] = "string:str8-utf8";
            map[(byte)0xa3] = "symbol:sym8";
            map[(byte)0xb0] = "binary:vbin32";
            map[(byte)0xb1] = "string:str32-utf8";
            map[(byte)0xb3] = "symbol:sym32";
            map[(byte)0xc0] = "list:list8";
            map[(byte)0xc1] = "map:map8";
            map[(byte)0xd0] = "list:list32";
            map[(byte)0xd1] = "map:map32";
            map[(byte)0xe0] = "array:array8";
            map[(byte)0xf0] = "array:array32";
        }

        public string this[byte key]
        {
            get
            {
                if (this.map.ContainsKey(key))
                {
                    return this.map[key];
                }
                else
                {
                    string res = String.Format("ERROR: Type {0} is not an AMQP type.", key);
                    return res;
                }
            }
        }

    }

    class Program
    {
        /// <summary>
        /// 
        /// </summary>
        static void SystemToAmqpTest(object value, Type valueType)
        {
            byte[] workBuffer = new byte[4096];
            ByteBuffer buffer = new ByteBuffer(workBuffer, 0, 0, workBuffer.Length);
            Amqp.Types.Encoder.WriteObject(buffer, value, false);

            AmqpTypeNames atm = new AmqpTypeNames();

            Console.WriteLine("| {0} | [0x{2}] {1} |", valueType, atm[workBuffer[0]], BitConverter.ToString(workBuffer, 0, 1));
        }
        static void SystemToAmqp()
        {
            System.Boolean v010 = true;
            System.Boolean v020 = false;
            System.Byte v030 = 3;
            System.Char v040 = 'a';
            System.DateTime v050 = new DateTime(2012, 01, 01);
            System.SByte v060 = -1;
            System.Byte v070 = 1;
            System.Int16 v080 = -2;
            System.UInt16 v090 = 2;
            System.Int32 v100 = -3;
            System.UInt32 v110 = 3;
            System.Int64 v120 = -4;
            System.UInt64 v130 = 4;

            System.Single v140 = 1.1F;
            System.Double v150 = 1.2;
            //System.Decimal v160 = 1.3M;

            System.Guid v170 = new Guid();

            System.Byte[] v180 = { 1, 2, 3 };
            // System.Byte[] v190 = new byte[4096]; fails

            Amqp.Types.List w01 = new Amqp.Types.List() { 100, "200" };
            Amqp.Types.Map w02 = new Amqp.Types.Map();
            w02[0] = "ABC";

            Console.WriteLine("[options=\"header\"]");
            Console.WriteLine("|====");
            Console.WriteLine("| Input User.NET Type | Output AMQP Type |");

            SystemToAmqpTest((object)v010, v010.GetType());
            SystemToAmqpTest((object)v020, v020.GetType());
            SystemToAmqpTest((object)v030, v030.GetType());
            SystemToAmqpTest((object)v040, v040.GetType());
            SystemToAmqpTest((object)v050, v050.GetType());
            SystemToAmqpTest((object)v060, v060.GetType());
            SystemToAmqpTest((object)v070, v070.GetType());
            SystemToAmqpTest((object)v080, v080.GetType());
            SystemToAmqpTest((object)v090, v090.GetType());
            SystemToAmqpTest((object)v100, v100.GetType());
            SystemToAmqpTest((object)v110, v110.GetType());
            SystemToAmqpTest((object)v120, v120.GetType());
            SystemToAmqpTest((object)v130, v130.GetType());
            SystemToAmqpTest((object)v140, v140.GetType());
            SystemToAmqpTest((object)v150, v150.GetType());
            //SystemToAmqpTest((object)v160, v160.GetType());
            SystemToAmqpTest((object)v170, v170.GetType());
            SystemToAmqpTest((object)v180, v180.GetType());
            //SystemToAmqpTest((object)v190, v190.GetType());

            SystemToAmqpTest((object)w01, w01.GetType());
            SystemToAmqpTest((object)w02, w02.GetType());
            Console.WriteLine("|====");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="bytes"></param>
        static void AmqpToSystemTest(byte[] bytes)
        {
            // Given some bytes, decode them and see what
            // system type is created
            ByteBuffer buffer = new ByteBuffer(bytes, 0, bytes.Length, bytes.Length);
            object value = new object();
            value = Amqp.Types.Encoder.ReadObject(buffer);

            AmqpTypeNames atm = new AmqpTypeNames();

            if (value != null)
            {
                Console.WriteLine("| [0x{0}] {1} | {2} |",
                    BitConverter.ToString(bytes, 0, 1),
                    atm[bytes[0]],
                    value.GetType());
            }
            else
            {
                Console.WriteLine("| [0x{0}] {1} | null |",
                    BitConverter.ToString(bytes, 0, 1),
                    atm[bytes[0]]);
            }
        }

        static void AmqpToSystem()
        {
            // These are the AMQP byte streams to be decoded
            byte[]       nullBin = new byte[] { 0x40 };
            byte[]   boolTrueBin = new byte[] { 0x41 };
            byte[]  boolFalseBin = new byte[] { 0x42 };
            byte[]      uint0Bin = new byte[] { 0x43 };
            byte[]     ulong0Bin = new byte[] { 0x44 };
            byte[]      list0Bin = new byte[] { 0x45 };
            byte[]      ubyteBin = new byte[] { 0x50, 0x33 };
            byte[]       byteBin = new byte[] { 0x51, 0xec };
            byte[]  uintSmallBin = new byte[] { 0x52, 0xe1 };
            byte[] ulongSmallBin = new byte[] { 0x53, 0xf2 };
            byte[]   intSmallBin = new byte[] { 0x54, 0xb3 };
            byte[]  longSmallBin = new byte[] { 0x55, 0x22 };
            byte[]  boolTrue1Bin = new byte[] { 0x56, 0x01 };
            byte[]     ushortBin = new byte[] { 0x60, 0x12, 0x34 };
            byte[]      shortBin = new byte[] { 0x61, 0x56, 0x78 };
            byte[]       uintBin = new byte[] { 0x70, 0xed, 0xcb, 0xa0, 0x98 };
            byte[]        intBin = new byte[] { 0x71, 0x56, 0x78, 0x9a, 0x00 };
            byte[]      floatBin = new byte[] { 0x72, 0xc2, 0xb1, 0xc2, 0x8f };
            byte[]       charBin = new byte[] { 0x73, 0x00, 0x00, 0x00, 0x41 };
            //byte[]  decimal32Bin = new byte[] { 0x74, 0x30, 0x92, 0xd6, 0x87 };
            byte[]      ulongBin = new byte[] { 0x80, 0x12, 0x34, 0x56, 0x78, 0xed, 0xcb, 0xa0, 0x98 };
            byte[]       longBin = new byte[] { 0x81, 0xff, 0xff, 0xff, 0xe6, 0x21, 0x42, 0xfe, 0x39 };
            byte[]     doubleBin = new byte[] { 0x82, 0x42, 0xd9, 0x43, 0x84, 0x93, 0xbc, 0x71, 0xce };
            byte[]         dtBin = new byte[] { 0x83, 0x00, 0x00, 0x01, 0x1d, 0x59, 0x8d, 0x1e, 0xa0 };
            //byte[]  decimal64Bin = new byte[] { 0x84, 0xb1, 0x04, 0x62, 0xd5, 0x3d, 0x21, 0x6e, 0xf4 };
            //byte[] decimal128Bin = new byte[] { 0x94, 0x30, 0x40, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff };
            byte[]       uuidBin = new byte[] { 0x98, 0xf2, 0x75, 0xea, 0x5e, 0x0c, 0x57, 0x4a, 0xd7, 0xb1, 0x1a, 0xb2, 0x0c, 0x56, 0x3d, 0x3b, 0x71 };
            byte[]         vbin8 = new byte[] { 0xA0, 0x01, 0x42 };
            byte[]   str8Utf8Bin = new byte[] { 0xa1, 0x04, 0x61, 0x6d, 0x71, 0x70 };
            byte[]       sym8Bin = new byte[] { 0xa3, 0x04, 0x61, 0x6d, 0x71, 0x70 };
            byte[]     vbin32Bin = new byte[] { 0xb0, 0x00, 0x00, 0x00, 0x01, 0x12, 0x23, 0x34, 0x45 };            
            byte[]  str32Utf8Bin = new byte[] { 0xb1, 0x00, 0x00, 0x00, 0x04, 0x61, 0x6d, 0x71, 0x70 };
            byte[]      sym32Bin = new byte[] { 0xb3, 0x00, 0x00, 0x00, 0x04, 0x61, 0x6d, 0x71, 0x70 };
            byte[]      list8Bin = new byte[] { 0xc0, 0x08, 0x02, 0x54, 0x64, 0xa1, 0x03, 0x32, 0x30, 0x30, 0x64, 0xa1, 0x03, 0x32, 0x30, 0x30 };
            byte[]       map8Bin = new byte[] { 0xc1, 0x08, 0x02, 0x54, 0x00, 0xa1, 0x03, 0x41, 0x42, 0x43, 0x00, 0xa1, 0x03, 0x41, 0x42, 0x43 };
            // list32 0xd0
            // map32 0xd1
            // array8 0xe0
            // array32 0xf0

            Console.WriteLine("[options=\"header\"]");
            Console.WriteLine("|====");
            Console.WriteLine("| Input AMQP Type | Output User .NET Type |");

            AmqpToSystemTest(nullBin);
            AmqpToSystemTest(boolTrueBin);
            AmqpToSystemTest(boolFalseBin);
            AmqpToSystemTest(uint0Bin);
            AmqpToSystemTest(ulong0Bin);
            AmqpToSystemTest(list0Bin);
            AmqpToSystemTest(ubyteBin);
            AmqpToSystemTest(byteBin);
            AmqpToSystemTest(uintSmallBin);
            AmqpToSystemTest(ulongSmallBin);
            AmqpToSystemTest(intSmallBin);
            AmqpToSystemTest(longSmallBin);
            AmqpToSystemTest(boolTrue1Bin);
            AmqpToSystemTest(ushortBin);
            AmqpToSystemTest(shortBin);
            AmqpToSystemTest(uintBin);
            AmqpToSystemTest(intBin);
            AmqpToSystemTest(floatBin);
            AmqpToSystemTest(charBin);
            //AmqpToSystemTest(decimal32Bin);
            AmqpToSystemTest(ulongBin);
            AmqpToSystemTest(longBin);
            AmqpToSystemTest(doubleBin);
            AmqpToSystemTest(dtBin);
            //AmqpToSystemTest(decimal64Bin);
            //AmqpToSystemTest(decimal128Bin);
            AmqpToSystemTest(uuidBin);
            AmqpToSystemTest(vbin8);
            AmqpToSystemTest(str8Utf8Bin);
            AmqpToSystemTest(sym8Bin);
            AmqpToSystemTest(vbin32Bin);
            AmqpToSystemTest(str32Utf8Bin);
            AmqpToSystemTest(sym32Bin);
            AmqpToSystemTest(list8Bin);
            AmqpToSystemTest(map8Bin);
            
            Console.WriteLine("|====");
        }

        static void Main(string[] args)
        {
            SystemToAmqp();
            AmqpToSystem();
        }
    }
}
