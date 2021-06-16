package mariadbcdc.connector.packet;

import mariadbcdc.connector.BinLogBadPacketException;
import mariadbcdc.connector.BinLogErrException;
import mariadbcdc.connector.CapabilityFlag;
import mariadbcdc.connector.io.ColumnDefPacket;
import mariadbcdc.connector.io.Either;
import mariadbcdc.connector.io.PacketIO;
import mariadbcdc.connector.io.ReadPacketData;
import mariadbcdc.connector.packet.connection.AuthSwitchRequestPacket;
import mariadbcdc.connector.packet.result.ColumnCountPacket;
import mariadbcdc.connector.packet.result.ResultSetPacket;
import mariadbcdc.connector.packet.result.TextResultSetRowPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ReadPacketReader {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private PacketIO packetIO;
    private byte[] buff = new byte[16777215];

    public ReadPacketReader(PacketIO packetIO) {
        this.packetIO = packetIO;
    }

    public ReadPacketData readPacketData() {
        int packetLength = packetIO.readInt(3);
        int sequenceNumber = packetIO.readInt(1);
        packetIO.readBytes(buff, 0, packetLength);

        ReadPacketData readPacketData = new ReadPacketData(packetLength, sequenceNumber, buff);

        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder();
            readPacketData.dump(sb);
            logger.trace("read packet data: {}", sb.toString());
        }
        return readPacketData;
    }

    public ReadPacket read(int clientCapabilities) {
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
        return UnknownReadPacket.INSTANCE;
    }

    public Either<OkPacket, ResultSetPacket> readResultSetPacket(int clientCapabilities) {
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

    private EofPacket readEofPacket() {
        ReadPacketData readPacketData = readPacketData();
        int firstBodyByte = readPacketData.getFirstByte();
        if (firstBodyByte == 0xFE && readPacketData.getRealPacketLength() <= 9) {
            return EofPacket.from(readPacketData);
        } else {
            throw new BinLogBadPacketException("not EOF packet");
        }
    }

}
