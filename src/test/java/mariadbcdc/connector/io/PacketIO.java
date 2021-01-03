package mariadbcdc.connector.io;

import mariadbcdc.connector.BinLogBadPacketException;
import mariadbcdc.connector.BinLogErrException;
import mariadbcdc.connector.CapabilityFlag;
import mariadbcdc.connector.packet.*;
import mariadbcdc.connector.packet.connection.AuthSwitchRequestPacket;
import mariadbcdc.connector.packet.result.ColumnCountPacket;
import mariadbcdc.connector.packet.result.ResultSetPacket;
import mariadbcdc.connector.packet.result.TextResultSetRowPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class PacketIO {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private final InputStream is;
    private final OutputStream os;

    private byte[] writeBody = new byte[16777215];
    private byte[] readBody = new byte[16777215];
    private int clientCapabilities;

    public PacketIO(InputStream is, OutputStream os) {
        this.is = is;
        this.os = os;
    }

    public void setClientCapabilities(int clientCapabilities) {
        this.clientCapabilities = clientCapabilities;
    }

    public ReadPacket read() {
        ReadPacketData readPacketData = readPacketData();
        int firstBodyByte = readPacketData.getFirstByte();
        if (firstBodyByte == 0xFE) {
            return AuthSwitchRequestPacket.from(readPacketData);
        }
        if (firstBodyByte == 0x00) {
            return OkPacket.from(readPacketData, clientCapabilities);
        }
        if (firstBodyByte == 0xFF) {
            return ErrPacket.from(readPacketData);
        }
        if (firstBodyByte == 0xFE && readPacketData.getRealPacketLength() <= 9) {
            return EofPacket.from(readPacketData);
        }
        return UnknownReadPcket.INSTANCE;
    }

    public ReadPacketData readPacketData() {
        ReadPacketData readPacketData = ReadPacketData.from(is, readBody);
        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            readPacketData.dump(sb);
            logger.debug("read packet data: {}", sb.toString());
        }
        return readPacketData;
    }

    private EofPacket readEofPacket() {
        ReadPacketData readPacketData = readPacketData();
        int firstBodyByte = readPacketData.getFirstByte();
        if (firstBodyByte == 0xFE && readPacketData.getRealPacketLength() <= 9) {
            return EofPacket.from(readPacketData);
        } else {
            throw new BinLogBadPacketException("not EOF packet");
        }
    }

    public Either<OkPacket, ResultSetPacket> readResultSetPacket() {
        ReadPacketData readPacketData = readPacketData();
        int firstBodyByte = readPacketData.getFirstByte();
        if (firstBodyByte == 0xFF) {
            ErrPacket errPacket = ErrPacket.from(readPacketData);
            throw new BinLogErrException(errPacket.toString());
        }
        if (firstBodyByte == 0x00) {
            return Either.left(OkPacket.from(readPacketData, clientCapabilities));
        }
        ColumnCountPacket columnCountPacket = ColumnCountPacket.from(readPacketData);
        ColumnDefPacket[] columnDefPackets = new ColumnDefPacket[columnCountPacket.getCount()];
        for (int i = 0; i < columnCountPacket.getCount(); i++) {
            ReadPacketData colDefReadPacket = readPacketData();
            columnDefPackets[i] = ColumnDefPacket.from(colDefReadPacket);
        }
        if (!CapabilityFlag.CLIENT_DEPRECATE_EOF.support(clientCapabilities)) {
            readEofPacket();
        }
        List<TextResultSetRowPacket> rows = new ArrayList<>();
        while (true) {
            ReadPacketData rowPacket = readPacketData();
            int first = rowPacket.getFirstByte();
            if (first == 0xFF) {
                ErrPacket errPacket = ErrPacket.from(readPacketData);
                throw new BinLogErrException(errPacket.toString());
            } else if (first == 0xFE) {
//                return CapabilityFlag.CLIENT_DEPRECATE_EOF.support(clientCapabilities) ?
//                        OkPacket.from(rowPacket, clientCapabilities) :
//                        EofPacket.from(rowPacket);
                break;
            }
            TextResultSetRowPacket row = TextResultSetRowPacket.from(columnDefPackets, rowPacket);
            rows.add(row);
        }
        return Either.right(
                new ResultSetPacket(columnCountPacket, columnDefPackets, rows)
        );
    }

    public void write(WritePacket respPacket) {
        BufferByteWriter writer = new BufferByteWriter(writeBody);
        respPacket.writeTo(writer);
        WritePacketData writePacketData1 = new WritePacketData(writer.getSequenceNumber(), writeBody, writer.getPacketLength());
        writePacketData1.send(os);
        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            writePacketData1.dump(sb);
            logger.debug("write packet data: {}", sb.toString());
        }
    }
}
