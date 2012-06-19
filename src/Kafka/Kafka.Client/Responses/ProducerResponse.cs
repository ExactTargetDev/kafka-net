// -----------------------------------------------------------------------
// <copyright file="ProducerResponse.cs" company="">
// TODO: Update copyright text.
// </copyright>
// -----------------------------------------------------------------------

using Kafka.Client.Exceptions;
using Kafka.Client.Serialization;

namespace Kafka.Client.Responses
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    public class ProducerResponse
    {
        public short VersionId { get; set; }
        public int CorrelationId { get; set; }
        public IEnumerable<short> Errors { get; set; }
        public IEnumerable<long> Offsets { get; set; }

        ProducerResponse(short versionId, int correlationId, IEnumerable<short> errors, IEnumerable<long> offsets)
        {
            this.VersionId = versionId;
            this.CorrelationId = correlationId;
            this.Errors = errors;
            this.Offsets = offsets;
        }

        public static ProducerResponse ParseFrom(KafkaBinaryReader reader)
        {
            //should I do anything withi this:
            int length = reader.ReadInt32();

            short errorCode = reader.ReadInt16();
            if (errorCode != KafkaException.NoError)
            {
                //ignore the error
            }
            var versionId = reader.ReadInt16();
            var correlationId = reader.ReadInt32();
            var numberOfErrors = reader.ReadInt32();
            var errors = new short[numberOfErrors];
            for (int i = 0; i < numberOfErrors; i++)
            {
                errors[i] = reader.ReadInt16();
            }
            var numberOfOffsets = reader.ReadInt32();
            var offsets = new long[numberOfOffsets];
            for (int i = 0; i < numberOfOffsets; i++)
            {
                offsets[i] = reader.ReadInt64();
            }
            return new ProducerResponse(versionId, correlationId, errors, offsets);
        }
    }
}
