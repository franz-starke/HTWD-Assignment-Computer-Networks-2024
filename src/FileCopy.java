import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/** @author Jörg Vogt */

/*
 * Remarks: UDP-checksum calculation, UDP-Lite RFC 3828
 * UDP checksum is calculated over IP-Pseudo-Header, UDP-Header, UDP-Data
 * No option to disable checksum in JAVA for UDP
 * UDP-Lite is part of Linux-kernel since 2.6.20
 * UDP-Lite support in java not clear
 */

public class FileCopy {
  static int port;
  static int delay;
  static double loss;
  static String dir = "upload/";

  public static void main(String[] args) throws IOException {
    Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    CustomLoggingHandler.prepareLogger(logger);
    /* set logging level
     * Level.CONFIG: default information (incl. RTSP requests)
     * Level.ALL: debugging information (headers, received packages and so on)
     */
    logger.setLevel(Level.CONFIG);
    logger.setLevel(Level.ALL);


    if (args.length != 2 && args.length !=4 && args.length !=5 ) {
      System.out.println("Usage: FileCopy server port [loss] [delay] ");
      System.out.println("Usage: FileCopy client host port file protocol");
      System.exit(1);
    }

    switch (args[0]) {
      case "client":
        String host = args[1];
        port = Integer.parseInt(args[2]);
        String fileName = args[3];
        String arqProt = "1";
        if (args.length == 5)
          arqProt = args[4];

        System.out.println("Client started for connection to: " + host + " at port " + port);
        System.out.println("Protokoll: " + arqProt);
        sendFile(host, port, fileName, arqProt);
        break;

      case "server":
        port = Integer.parseInt(args[1]);
        if (args.length == 4) {
          loss = Double.parseDouble(args[2]);
          delay = Integer.parseInt(args[3]);
        }
        System.out.println("Server started at port: " + port);
        handleConnection(port);
        break;
      default:
    }
  }

  private static void sendFile(String host, int port, String fileName, String arq)
      throws IOException {
    // establish socket - exception possible
    // TODO Exception handling
    Socket socket = new Socket(host, port);
    FileTransfer myFT = new FileTransfer(host, socket, fileName, arq);
    boolean c = myFT.file_req();
    if (c) {
      System.out.println("Client-AW: Ready");
      System.exit(0);
    }
    else {
      System.out.println("Client-AW: Abort because of maximum retransmission");
      System.exit(1);
    }
  }

  private static void handleConnection(int port) throws IOException {
    // establish connection
    Socket socket = new Socket(port, loss, delay);
    FileTransfer myFT = new FileTransfer(socket, dir);
    do {
      if (myFT.file_init() ) System.out.println("Server-AW: file received");
      else System.out.println("Server-AW: file receive abort (time out)");
    } while (true);
  }
}
