#!/bin/bash
#
# Generate some amqp-types messages to a private queue for Lite analysis.
# This script leverages the qpid-proton-cpp sender shim to generate/send messages.

broker="localhost:5672"
queue="jms.queue.qpid-interop.lite"
QPID_INTEROP_TEST_HOME="/opt/local/lib/python2.7/site-packages/qpid_interop_test"
sender=${QPID_INTEROP_TEST_HOME}/shims/qpid-proton-cpp/amqp_types_test/Sender

# This example generates four over-the-wire messages
#
# ${sender} ${broker} ${queue} 'ubyte' '["0x0", "0x7f", "0x80", "0xff"]'
#

${sender} ${broker} ${queue} 'null'        '["None"]'
${sender} ${broker} ${queue} 'boolean'     '["True"]'
${sender} ${broker} ${queue} 'ubyte'       '["0x7f"]'
${sender} ${broker} ${queue} 'ushort'      '["0x7fff"]'
${sender} ${broker} ${queue} 'uint'        '["0x7fffffff"]'
${sender} ${broker} ${queue} 'ulong'       '["0x102030405"]'
${sender} ${broker} ${queue} 'byte'        '["0x42"]'
${sender} ${broker} ${queue} 'short'       '["0x4242"]'
${sender} ${broker} ${queue} 'int'         '["0x42424242"]'
${sender} ${broker} ${queue} 'long'        '["0x4242424242424242"]'
${sender} ${broker} ${queue} 'float'       '["0x40490fdb"]'
${sender} ${broker} ${queue} 'double'      '["0x400921fb54442eea"]'
${sender} ${broker} ${queue} 'timestamp'   '["0xdc6be25480"]'
${sender} ${broker} ${queue} 'uuid'        '["00010203-0405-0607-0809-0a0b0c0d0e0f"]'
${sender} ${broker} ${queue} 'binary'      '["12345"]'
${sender} ${broker} ${queue} 'string'      '["Hello, world"]'
${sender} ${broker} ${queue} 'symbol'      '["myDomain.123"]'
${sender} ${broker} ${queue} 'list'        '[["ubyte:1", "int:-2", "float:3.14"]]'
${sender} ${broker} ${queue} 'map'         '[{"string:one": "ubyte:1", "string:two": "ushort:2"}]'

#${sender} ${broker} ${queue} 'char'        '["A"]'
#${sender} ${broker} ${queue} 'decimal32'   '["0x40490fdb"]'
#${sender} ${broker} ${queue} 'decimal64'   '["0x400921fb54442eea"]'
#${sender} ${broker} ${queue} 'decimal128'  '["0xff0102030405060708090a0b0c0d0e0f"]'
