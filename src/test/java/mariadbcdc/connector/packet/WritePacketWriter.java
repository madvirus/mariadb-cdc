package mariadbcdc.connector.packet;

import mariadbcdc.connector.io.BufferByteWriter;
import mariadbcdc.connector.io.PacketIO;
import mariadbcdc.connector.io.WritePacketData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WritePacketWriter {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private byte[] writeBody = new byte[16777215];

    private PacketIO packetIO;

    public WritePacketWriter(PacketIO packetIO) {
        this.packetIO = packetIO;
    }

    public void write(WritePacket respPacket) {
        BufferByteWriter writer = new BufferByteWriter(writeBody);
        respPacket.writeTo(writer);
        WritePacketData writePacketData = new WritePacketData(writer.getSequenceNumber(), writeBody, writer.getPacketLength());
        writePacketData.send(packetIO);
        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            writePacketData.dump(sb);
            logger.debug("write packet data: {}", sb.toString());
        }
    }

}
