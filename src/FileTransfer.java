import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.System.exit;

public class FileTransfer implements  FT{

    private ARQ myARQ;
    Logger logger;
    File file;
    long file_size;
    long delta_time;
    java.util.zip.CRC32 file_y = new java.util.zip.CRC32();
    Boolean next_last_transmit = false;
    public FileTransfer(String host, Socket socket, String fileName, String arq) {
        Random r = new Random();
        int session_number = r.nextInt(65536);
        file = new File(fileName);
        myARQ = new SW(socket,session_number);
        logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
        logger.log(Level.CONFIG,"FileTransfer: " + arq + " " + fileName + " " + host);
    }

    public FileTransfer(Socket socket, String dir) {
        myARQ = new SW(socket);
        logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
        logger.log(Level.CONFIG,"Server-FileTransfer: " + " " + dir);
    }

    public static byte[] combineByteArray(final byte[] array1, byte[] array2) {
        byte[] joinedArray = Arrays.copyOf(array1, array1.length + array2.length);
        System.arraycopy(array2, 0, joinedArray, array1.length, array2.length);
        return joinedArray;
    }

    public static byte [] trim_end(final byte[] array){
        int last_none_null = 0;
        for(int i=0; i < array.length;i++){
            if(array[i] != 0){
                last_none_null = i;
            }
        }
        return Arrays.copyOfRange(array, 0, last_none_null+1);
    }

    // Creating start package
    private boolean first_package() throws IOException {
        byte[] start_package_data = "Start".getBytes(StandardCharsets.US_ASCII);

        // Handle file work
        long file_length = file.length();
        short file_name_length = (short) file.getName().length();
        String file_name = file.getName();

        byte[] file_length_byte_array = ByteBuffer.allocate(8).putLong(file_length).array();
        byte[] file_name_length_byte_array = ByteBuffer.allocate(2).putShort(file_name_length).array();

        // Combining all byte arrays to packet
        start_package_data = combineByteArray(start_package_data,file_length_byte_array);
        start_package_data = combineByteArray(start_package_data,file_name_length_byte_array);
        start_package_data = combineByteArray(start_package_data,file_name.getBytes(StandardCharsets.UTF_8));

        // Calculating crc32 value
        java.util.zip.CRC32 x = new java.util.zip.CRC32();
        x.update(start_package_data);
        int crc323_value = (int) x.getValue();

        byte[] crc323_value_byte_array = ByteBuffer.allocate(4).putInt(crc323_value).array();
        start_package_data = combineByteArray(start_package_data,crc323_value_byte_array);

        // Sending package
        boolean success = myARQ.data_req(start_package_data,start_package_data.length,false);
        logger.log(Level.CONFIG,"Client-FileTransfer: Start send");
        return success;
    }

    // Creating data packet
    private boolean data_package() throws IOException {
        long start_time = System.currentTimeMillis();
        long last_time = System.currentTimeMillis();
        // Create 1400 byte buffer
        FileInputStream input_stream = new FileInputStream(file);
        byte[] data_package_data = new byte[1400];
        boolean success = false;
         int step = 0;
        // Loop until file is read
        while (true) {
            long current_time = System.currentTimeMillis();
            delta_time += current_time-last_time;
            last_time = current_time;
            step++;
            if (delta_time > 1000){
                delta_time -= 1000;
                logger.log(Level.CONFIG,"Datenrate: "+ ((step* 1400L))/(float) (current_time-start_time)*1000 + " bytes/s");
            }
            int read = input_stream.read(data_package_data);
            if (read == -1) break;

            byte[] data_package_array = Arrays.copyOfRange(data_package_data, 0, read);
            for (int i = 0; i < 10; i++) {
                success = myARQ.data_req(data_package_array, data_package_array.length, false);
                if (success) {
                    break;
                }
            }
            if (!success) return false;

            if (read < 1400) {
                input_stream.close();
                break;
            }
        }
        return true;
    };

    // Creating last package
    private boolean last_package() throws IOException {

        java.util.zip.CRC32 read_file_x = new java.util.zip.CRC32();
        FileInputStream read_file_stream = new FileInputStream(file);

        // Reading new crc32 value
        while (true) {
            byte[] data_package_data = new byte[1400];
            int read = read_file_stream.read(data_package_data);
            read_file_x.update(Arrays.copyOfRange(data_package_data, 0, read));
            if (read != 1400) {
                read_file_stream.close();
                break;
            }
        }

        return myARQ.data_req(ByteBuffer.allocate(4).putInt((int) read_file_x.getValue()).array(), 4, true);
    };

    // Client
    @Override
    public boolean file_req() throws IOException {
        // Send start package max 10 times
        for (int i = 0; i < 10; i++) {
            if (first_package()) {
                break;
            } else if (i == 9){
                return false;
            }
        }

        // Send data packages
        boolean y = data_package();
        if (!y) return false;

        // Send last package
        for (int i = 0; i < 10; i++) {
            if (last_package()) {
                break;
            } else if (i == 9){
                return false;
            }
        }
        return true;
    }

    // Server
    @Override
    public boolean file_init() throws IOException {
        byte[] data;
        try {
            if (next_last_transmit) {
                data = myARQ.data_ind_req((int) file_y.getValue());
            } else {
                data = myARQ.data_ind_req();
            }
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }

        int state = 1;
        String start_marker = "";

        // Create copy of data (for crc), because original data is edited
        byte[] data_copy = Arrays.copyOfRange(data, 0, data.length);

        // Getting data, that every package has
        int sessionID = Short.toUnsignedInt(ByteBuffer.wrap(data).getShort()); // Not used but nice to have
        data = Arrays.copyOfRange(data, 1, data.length);

        byte packetid = (byte) Byte.toUnsignedInt((byte) ByteBuffer.wrap(data).getChar());
        data = Arrays.copyOfRange(data, 2, data.length);

        // Start marker
        if (packetid == 0 && data.length >= 5) {
            start_marker = new String(data,0,5);
            if (start_marker.equals("Start")) {
                data = Arrays.copyOfRange(data, 5, data.length);
                state = 0;
            }
        }

        if (next_last_transmit) state = 2;

        // State route for start package
        if (state == 0) {
            int data_length = 18;

            // Getting file infos
            file_size = ByteBuffer.wrap(data).getLong();
            data = Arrays.copyOfRange(data, 8, data.length);

            short file_name_length = (short) Short.toUnsignedInt(ByteBuffer.wrap(data).getShort());
            data = Arrays.copyOfRange(data, 1, data.length);
            data_length += file_name_length;

            String file_name = new String(data,1,file_name_length);
            data = Arrays.copyOfRange(data, file_name_length+1, data.length);

            // Crc calculation
            int crc32 = (int) Integer.toUnsignedLong(ByteBuffer.wrap(Arrays.copyOfRange(data, 0, 4)).getInt());

            java.util.zip.CRC32 x = new java.util.zip.CRC32();
            x.update(Arrays.copyOfRange(data_copy, 3, data_length));
            int crc323_value = (int) x.getValue();

            logger.log(Level.CONFIG,"Server-FileTransfer: received");

            if (crc32 == crc323_value) {
                // Create new file
                file = new File(file_name);
                if (!file.createNewFile()) {
                    int i = 1;
                    boolean exists = false;
                    while (!exists) {
                        if (file_name.contains("."))
                            file = new File(file_name.split("\\.")[0]+" "+i+"."+file_name.split("\\.")[1]);
                        else {
                            file = new File(file_name + " " + i);
                        }
                        if (!file.createNewFile()) {
                            i++;
                        } else {
                            exists = true;
                        }
                    }
                }
            } else {
                return false;
            }
            return true;

        // State route for data package
        } else if (state == 1) {

            // Write to file
            FileOutputStream file_Output_Stream = new FileOutputStream(file,true);
            file_Output_Stream.write(data);
            file_y.update(data);
            file_Output_Stream.close();
            if (file.length() == file_size){
                next_last_transmit = true;
            }

        // State route for last package
        } else {
            long last_crc32 = Integer.toUnsignedLong(ByteBuffer.wrap(Arrays.copyOfRange(data, 0, data.length)).getInt());
            if (file_y.getValue() == last_crc32) exit(0);
        }
        return true;
    }
}
