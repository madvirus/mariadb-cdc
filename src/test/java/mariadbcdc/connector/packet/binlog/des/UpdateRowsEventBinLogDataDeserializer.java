package mariadbcdc.connector.packet.binlog.des;

import mariadbcdc.connector.io.PacketIO;
import mariadbcdc.connector.packet.binlog.BinLogData;
import mariadbcdc.connector.packet.binlog.BinLogHeader;
import mariadbcdc.connector.packet.binlog.BinLogStatus;

public class UpdateRowsEventBinLogDataDeserializer implements BinLogDataDeserializer {
    @Override
    public BinLogData deserialize(PacketIO packetIO, BinLogStatus binLogStatus, BinLogHeader header) {
        packetIO.skipRemaining();

        // uint<6> The table id
        long tableId = packetIO.readLong(6);
        //uint<2> Flags
        int flags = packetIO.readInt(2);
        //uint<lenenc> Number of columns
        int numberOfColumns = packetIO.readLengthEncodedInt();
        //byte<n>Columns used. n = (number_of_columns + 7)/8
        //if (event_type == UPDATE_ROWS_EVENT_v1
        //  byte<n> Columns used (Update). n = (number_of_columns + 7)/8
        //byte<n> Null Bitmap (n = (number_of_columns + 7)/8)
        //string<len> Column data. The length needs to be calculated by checking the column types from referring TABLE_MAP_EVENT.
        //if (event_type == UPDATE_ROWS_EVENT_v1
        //  byte<n> Null Bitmap_Update. n = (number_of_columns + 7)/8
        //  string<len> Update Column data. The length needs to be calculated by checking the used colums bitmap and column types from referring TABLE_MAP_EVENT.

        return null;
    }
}
