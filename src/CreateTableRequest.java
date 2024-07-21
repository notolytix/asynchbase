package org.hbase.async;

import com.google.protobuf.ByteString;
import org.hbase.async.generated.ClientPB;
import org.hbase.async.generated.HBasePB;
import org.hbase.async.generated.MasterPB;
import org.jboss.netty.buffer.ChannelBuffer;

import java.nio.charset.StandardCharsets;

public class CreateTableRequest extends BatchableRpc
        implements HBaseRpc.HasTable, HBaseRpc.HasKey,
        HBaseRpc.HasFamily, HBaseRpc.HasQualifiers {

    private static final byte [] CREATE = new byte [] {'c', 'r', 'e', 'a', 't', 'e'};
    private long lockid = RowLock.NO_LOCK;
    private byte [] compression;

    public CreateTableRequest(byte[] table, byte[] key, byte[] family, long timestamp, long lockid) {
        super(table, key, family, timestamp, lockid);
        System.out.println("guardian create CreateTableRequest");
    }

    public CreateTableRequest(byte[] table, byte[] family, long timestamp, byte [] compression) {
        super(table, new byte[0], family, timestamp, RowLock.NO_LOCK);
        this.compression = compression;
        System.out.println("guardian create CreateTableRequest");
    }
//
//    /** Serializes this request.  */
//    ChannelBuffer serialize(final byte server_version) {
////        if (server_version < RegionClient.SERVER_VERSION_095_OR_ABOVE) {
////            return serializeOld(server_version);
////        }
//
//        final ClientPB.CreateTableRequest.Builder get = ClientPB.CreateTableRequest.newBuilder()
//                .setRegion(region.toProtobuf())
//                .setCreateTable(getP().build());
//        System.out.println("serialize Create request " + toChannelBuffer(GetRequest.GGET, get.build()).toString());
//        return toChannelBuffer(GetRequest.GGET, get.build());
//    }

    @Override
    ChannelBuffer serialize(byte b) {
        System.out.println("guardian CreateTableRequest serialize");
//        return null;

//        final ClientPB.GetRequest.Builder get = ClientPB.GetRequest.newBuilder()
//                .setRegion(region.toProtobuf())
//                .setGet(getPB().build());
//
//        return toChannelBuffer(GetRequest.GGET, get.build());
//    }
        HBasePB.ColumnFamilySchema columnFamilySchema = HBasePB.ColumnFamilySchema.newBuilder().setName(ByteString.copyFrom("cf".getBytes())).build();
        org.hbase.async.generated.HBasePB.TableName tableName = HBasePB.TableName.newBuilder().setQualifier(ByteString.copyFrom("rossiTable".getBytes()))
                .build();
        HBasePB.TableSchema table = HBasePB.TableSchema.newBuilder()
                .setTableName(tableName)
                .setColumnFamilies(0, columnFamilySchema).build();
        MasterPB.CreateTableRequest.Builder createTable = MasterPB.CreateTableRequest.newBuilder()
                .setTableSchema(table);
        org.jboss.netty.buffer.ChannelBuffer buf  = toChannelBuffer(CREATE, createTable.build());
        System.out.println("serialize ");
        return toChannelBuffer(CREATE, createTable.build());
    }
//
//    ClientPB.CreateTable.Builder getPB() {
//        System.out.println("guardian getPB");
//        final ClientPB.CreateTable.Builder create = ClientPB.CreateTable.newBuilder()
//                .setName(Bytes.wrap(table));
//
//        if (family != null) {
////            final ClientPB.CreateTable.Builder create = ClientPB.Column.newBuilder();
////            column.setFamily(Bytes.wrap(family));
//            System.out.println("set family");
//            create.setFamily(Bytes.wrap("cf".getBytes(StandardCharsets.UTF_8)));
//
////            if (qualifiers != null) {
////                for (final byte[] qualifier : qualifiers) {
////                    column.addQualifier(Bytes.wrap(qualifier));
////                }
////            }
////            getpb.addColumn(column.build());
//        }
//        if (compression != null) {
//            create.setCompression(Bytes.wrap(compression));
//        }
//        return create;
//    }


    @Override
    Object deserialize(ChannelBuffer channelBuffer, int i) {
        System.out.println("guardian deserialize CreateTableRequest......");
        return null;
    }

    @Override
    byte[] method(byte b) {
        System.out.println("guardian create method");
        return CREATE;
    }

    @Override
    ClientPB.MutationProto toMutationProto() {
        System.out.println("guardian toMutationProto");
        return null;
    }

    @Override
    byte version(byte server_version) {
        return 0;
    }

    @Override
    byte code() {
        return 0;
    }

    @Override
    int numKeyValues() {
        return 0;
    }

    @Override
    int payloadSize() {
        return 0;
    }

    @Override
    void serializePayload(ChannelBuffer buf) {
        System.out.println("guardian serializePayload");
    }

    @Override
    public byte[] key() {
        return new byte[0];
    }

    @Override
    public byte[][] qualifiers() {
        return new byte[0][];
    }

    @Override
    public byte[] table() {
        return new byte[0];
    }
}
