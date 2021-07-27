package mariadbcdc.binlog.reader.packet;

import mariadbcdc.binlog.reader.BinLogBadPacketException;
import mariadbcdc.binlog.reader.BinLogErrException;
import mariadbcdc.binlog.reader.CapabilityFlag;
import mariadbcdc.binlog.reader.io.ColumnDefPacket;
import mariadbcdc.binlog.reader.io.Either;
import mariadbcdc.binlog.reader.io.PacketIO;
import mariadbcdc.binlog.reader.io.ReadPacketData;
import mariadbcdc.binlog.reader.packet.connection.AuthSwitchRequestPacket;
import mariadbcdc.binlog.reader.packet.result.ColumnCountPacket;
import mariadbcdc.binlog.reader.packet.result.ResultSetPacket;
import mariadbcdc.binlog.reader.packet.result.TextResultSetRowPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReadPacketReader {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private static final int MAX_PACKET_LENGTH = 16777215;

    private PacketIO packetIO;
    private byte[] buff = new byte[MAX_PACKET_LENGTH];


    public ReadPacketReader(PacketIO packetIO) {
        this.packetIO = packetIO;
    }

    public ReadPacketData readPacketData() {
        int packetLength = packetIO.readInt(3);
        int sequenceNumber = packetIO.readInt(1);

        ReadPacketData readPacketData = packetLength == MAX_PACKET_LENGTH ?
                readMultiplePacketChunk(packetLength, sequenceNumber) :
                readSinglePacketChunk(packetLength, sequenceNumber);

        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder();
            readPacketData.dump(sb);
            logger.trace("read packet data: {}", sb.toString());
        }
        return readPacketData;
    }

    private ReadPacketData readSinglePacketChunk(int packetLength, int sequenceNumber) {
        packetIO.readBytes(buff, 0, packetLength);
        return new ReadPacketData(packetLength, sequenceNumber, buff);
    }

    private ReadPacketData readMultiplePacketChunk(int packetLength, int sequenceNumber) {
        byte[] result = new byte[packetLength];
        packetIO.readBytes(result, 0, packetLength);
        int chunkLen;
        do {
            chunkLen = packetIO.readInt(3);
            packetIO.readInt(1);
            result = Arrays.copyOf(result, result.length + chunkLen);
            packetIO.readBytes(result, result.length - chunkLen, chunkLen);
        } while (chunkLen == MAX_PACKET_LENGTH);
        return new ReadPacketData(result.length, sequenceNumber, result);
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
