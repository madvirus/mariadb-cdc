package mariadbcdc.connector.packet.binlog.des;

import mariadbcdc.connector.io.PacketIO;
import mariadbcdc.connector.packet.binlog.BinLogData;
import mariadbcdc.connector.packet.binlog.BinLogHeader;
import mariadbcdc.connector.packet.binlog.BinLogStatus;

public class UpdateRowsEventBinLogDataDeserializer implements BinLogDataDeserializer {
    @Override
    public BinLogData deserialize(PacketIO packetIO, BinLogStatus binLogStatus, BinLogHeader header) {
        //packetIO.skipRemaining();

        // 0  1  2  3  4  5  6  7   8  9  A  B  C  D  E  F |01234567 89ABCDEF|
        //----------------------- ------------------------ |-------- --------|
        //26 00 00 00 00 00 01 00  06*FF FF DC 01 00 00 00 |........ .???....|
        //04 6E 61 6D 65 99 A8 8D  5B E1 DC 01 00 00 00 12 |.name... .??.....|
        //6E 61 6D 65 32 30 32 31  30 31 30 36 32 31 34 37 |name2021 01062147|
        //33 36 99 A8 8D 5B E1 F1  B9 57 57                |36....?? .WW

        /** uint<6> The table id */
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
