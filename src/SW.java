import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.logging.Level;

import static java.lang.System.exit;

public class SW extends ARQAbst{

    //Client
    public SW(Socket socket) {
        super(socket);
    }

    // Server
    public SW(Socket socket, int sessionID) {
        super(socket, sessionID);
    }

    @Override
    public boolean data_req(byte[] hlData, int hlSize, boolean lastTransmission) {
        byte[] data = generateDataPacket(hlData,hlSize);
        socket.sendPacket(data);
        logger.log(Level.CONFIG,"Client-SW: Data send");
        return waitForAck(pNr);
    }

    @Override
    protected boolean waitForAck(int packetNr) {
        socket.setTimeout(1000);
        DatagramPacket ackPacket;
        try {
            ackPacket = socket.receivePacket();
        } catch (TimeoutException e) {
            return false;
        }

        // Getting package data
        byte[] data = ackPacket.getData();
        int ack_sessionID = Short.toUnsignedInt(ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN).getShort());
        int ack_packetid = ackPacket.getData()[2];
        ack_packetid = ack_packetid & 0x000000ff;

        logger.log(Level.CONFIG, "Client-SW: Marker received");

        if (sessionID == ack_sessionID && ack_packetid == pNr){
            pNr ++;
            return true;
        } else {
            return false;
        }
    }

    @Override
    protected int getPacketNr(DatagramPacket packet) {
        return packet.getData()[2];
    }

    @Override
    protected void getAckData(DatagramPacket packet) {}

    @Override
    protected int getSessionID(DatagramPacket packet) {
        return Short.toUnsignedInt(ByteBuffer.wrap(packet.getData()).order(ByteOrder.BIG_ENDIAN).getShort());
    }

    public static byte[] combineByteArray(final byte[] array1, byte[] array2) {
        byte[] joinedArray = Arrays.copyOf(array1, array1.length + array2.length);
        System.arraycopy(array2, 0, joinedArray, array1.length, array2.length);
        return joinedArray;
    }

    @Override
    protected byte[] generateDataPacket(byte[] sendData, int dataSize) {
        pNr = pNr%256;

        byte[] pNr_byte_array = {(byte) pNr};
        byte[] sessionID_byte_array = ByteBuffer.allocate(2).putShort((short) sessionID).array();

        sendData = combineByteArray(pNr_byte_array,sendData);
        sendData = combineByteArray(sessionID_byte_array,sendData);
        return sendData;
    }

    // Server
    @Override
    public byte[] data_ind_req(int... values) throws TimeoutException {
        DatagramPacket dataPacket;
        dataPacket = socket.receivePacket();
        logger.log(Level.CONFIG,"Server-SW: Data received");
        sessionID = getSessionID(dataPacket);
        if (values.length > 0) {
            sendAckCrC(getPacketNr(dataPacket),values[0]);
        } else {
            sendAck(getPacketNr(dataPacket));
        }
        return Arrays.copyOfRange(dataPacket.getData(), 0, dataPacket.getLength());
    }

    public void sendAckCrC(int nr, int crc){
        socket.sendPacket(generateLastAckPacket(nr,crc));
    }

    @Override
    byte[] generateAckPacket(int packetNr) {
        byte[] sendData = {(byte) packetNr};
        byte[] sessionID_byte_array = ByteBuffer.allocate(2).putShort((short) sessionID).array();

        sendData = combineByteArray(sessionID_byte_array,sendData);
        return sendData;
    }

    byte[] generateLastAckPacket(int packetNr, int crc) {
        byte[] sendData = {(byte) packetNr};
        byte[] sessionID_byte_array = ByteBuffer.allocate(2).putShort((short) sessionID).array();
        byte[] crc_byte_array = ByteBuffer.allocate(4).putInt(crc).array();

        sendData = combineByteArray(sessionID_byte_array,sendData);
        sendData = combineByteArray(sendData,crc_byte_array);
        return sendData;
    }

    @Override
    void sendAck(int nr) {
        socket.sendPacket(generateAckPacket(nr));
    }

    @Override
    boolean checkStart(DatagramPacket packet) {
        return false;
    }

    @Override
    public void closeConnection() {
    }
}
