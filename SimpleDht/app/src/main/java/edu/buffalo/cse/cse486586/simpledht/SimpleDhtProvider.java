package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    private static final int SERVER_PORT = 10000;
    static String myPort = "";
    static String myPortHash = "";
    static String next = "";
    static String nextHash = "";
    static String prev = "";
    static String prevHash = "";
    static List<String[]> records = new ArrayList<>();
    List<String> savedPorts = new ArrayList<>();
    Set<String> activePorts = new HashSet();
    Map<String, String> sortedHashesToPort = new HashMap<>();
    List<String> sortedHashes = new ArrayList<>();
    private String LIVECHECK = "LIVECHECK";
    private String INSERT = "INSERT";
    private String QUERY = "QUERY";
    private String DELETE = "DELETE";
    private String DELIMITER = "#";


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        records.clear();
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        setActivePorts();

        Log.e(TAG, "Active Ports found :" + activePorts);

        if (activePorts.size() > 1) {
            formChord();
        }

        // TODO Auto-generated method stub
        try {
            if (activePorts.size() <= 1) {
                String actual = values.getAsString("key");
                Log.e(TAG, "Active Ports < 1, Key : " + actual);
                String key = genHash(values.getAsString("key"));
                String value = values.getAsString("value");
                String[] record = new String[]{actual, key, value};
                records.add(record);
            } else {
                String recordKey = values.getAsString("key");
                String recordHash = genHash(recordKey);
                String recordValue = values.getAsString("value");
                Log.e(TAG, "Key " + recordKey + ", Key Hash : " + recordHash + ", My PredHash : " + prevHash + ", MyHash: " + myPortHash + ", My nextHash : " + nextHash);
                Log.e(TAG, " My Pred : " + prev + ", MyPort: " + myPort + ", My next : " + next);
                if (canInsertWithInMyNode(recordHash)) {
                    Log.e(TAG, "Lies with in my port" + myPort);
                    String[] record = new String[]{recordKey, recordHash, recordValue};
                    records.add(record);
                } else {
                    //TODO : Transport the message to server
                    StringBuffer message = new StringBuffer();
                    message.append(INSERT).append(DELIMITER).append(recordHash).
                            append(DELIMITER).append(recordKey).append(DELIMITER).
                            append(recordValue);
                    try {
                        Log.e(TAG, message.toString() + " Passing to Server from insert:" + next);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(next));
                        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                        dos.writeUTF(message.toString());
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        String port = String.valueOf((Integer.parseInt(portStr) * 2));
        myPort = port;
        savedPorts = declarePorts();
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub

//        setActivePorts();
//        if (activePorts.size() > 1) {
//            formChord();
//        }

        MatrixCursor cursor = getCursor();
        if (activePorts.size() <= 1 || selection.equals("@")) {
            switch (selection) {
                case "*":
                case "@":
                    for (String[] each : records) {
                        MatrixCursor.RowBuilder rb = cursor.newRow();
                        rb.add("key", each[0]);
                        rb.add("value", each[2]);
                    }
                    return cursor;
                default:
                    MatrixCursor.RowBuilder rb = cursor.newRow();
                    for (String[] each : records) {
                        if (each[0].equals(selection)) {
                            rb.add("key", each[0]);
                            rb.add("value", each[2]);
                        }
                    }
                    return cursor;
            }
        } else {
            StringBuffer message = new StringBuffer();
            message.append("QUERY").append(DELIMITER);
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(next));
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                DataInputStream dis = new DataInputStream(socket.getInputStream());
                String response = "";
                switch (selection) {
                    case "*":
                        message.append("*").append(DELIMITER).append(prev);
                        dos.writeUTF(message.toString());
                        dos.flush();
                        response = dis.readUTF();
                        StringBuffer resultBuffer = new StringBuffer(response);
                        for (String[] record : records) {
                            String key = record[0];
                            String value = record[2];
                            resultBuffer.append(key).append("=").append(value);
                            resultBuffer.append(";");
                        }
                        resultBuffer.deleteCharAt(resultBuffer.length() - 1);
                        String[] splits = resultBuffer.toString().split(";");
                        for (String each : splits) {
                            String[] record = each.split("=");
                            MatrixCursor.RowBuilder rb = cursor.newRow();
                            rb.add("key", record[0]);
                            rb.add("value", record[1]);
                        }
                        break;
                    default:
                        boolean hasResult = false;
                        for (String[] record : records) {
                            if (record[0].equals(selection)) {
                                hasResult = true;
                                response = record[2];
                                break;
                            }
                        }
                        if (hasResult) {
                            MatrixCursor.RowBuilder rb = cursor.newRow();
                            rb.add("key", selection);
                            rb.add("value", response);
                        } else {
                            message.append(selection).append(DELIMITER).append(prev);
                            Log.e(TAG, "Passing Query:" + message + "to server :" + next);
                            dos.writeUTF(message.toString());
                            dos.flush();
                            response = dis.readUTF();
                            MatrixCursor.RowBuilder rb = cursor.newRow();
                            rb.add("key", selection);
                            rb.add("value", response);
                        }
                        break;
                }
                socket.close();
            } catch (Exception e) {

            }


            return cursor;
        }


    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {

                while (true) {
                    Socket socket = serverSocket.accept();
                    try {
                        DataInputStream dis = new DataInputStream(socket.getInputStream());
                        String msgReceieved = dis.readUTF();
                        Log.e(TAG, "Received message " + msgReceieved);
                        String[] splits = msgReceieved.split(DELIMITER);
                        Log.e(TAG, "Message Tpe : " + splits[0]);
                        switch (splits[0].trim()) {
                            case "LIVECHECK":
                                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                                dos.writeUTF("ROGER THAT");
                                dos.flush();
                                break;
                            case "INSERT":
                                String recordHash = splits[1];
                                String recordKey = splits[2];
                                String recordValue = splits[3];
                                setActivePorts();
                                formChord();
//                                Log.e(TAG, "Log from Server :" + myPort);
//                                Log.e(TAG, "Key " + recordKey + ", Key Hash : " + recordHash + ", My PredHash : " + prevHash + ", MyHash: " + myPortHash + ", My nextHash : " + nextHash);
//                                Log.e(TAG, " My Pred : " + prev + ", MyPort: " + myPort + ", My next : " + next);
                                if (canInsertWithInMyNode(recordHash)) {
                                    Log.e(TAG, "This hash is with in my Range and inserted");
                                    String[] record = new String[]{recordKey, recordHash, recordValue};
                                    records.add(record);
                                } else {
                                    Socket nextSock = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(next));
                                    Log.e(TAG, "Passing to successor socket :" + next);
                                    DataOutputStream insertDos = new DataOutputStream(nextSock.getOutputStream());
                                    insertDos.writeUTF(msgReceieved);
                                    insertDos.flush();
                                }
                                break;
                            case "QUERY":
                                String query = splits[1];
                                String lastPort = splits[2];
//                                setActivePorts();
//                                formChord();
                                DataOutputStream respSock = new DataOutputStream(socket.getOutputStream());
                                switch (splits[1]) {
                                    case "*":
                                        if (myPort.equals(lastPort)) {
                                            StringBuffer resultBuffer = new StringBuffer();
                                            for (String[] record : records) {
                                                String key = record[0];
                                                String value = record[2];
                                                resultBuffer.append(key).append("=").append(value);
                                                resultBuffer.append(";");
                                            }
                                            Log.e(TAG, "Writing for the firstTime : " + myPort);
                                            respSock.writeUTF(resultBuffer.toString());
                                        } else {
                                            Socket nextSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                    Integer.parseInt(next));
                                            DataOutputStream nextRespSock = new DataOutputStream(nextSocket.getOutputStream());
                                            DataInputStream nextReqSock = new DataInputStream(nextSocket.getInputStream());
                                            Log.e(TAG, "Passing * to next socket :" + socket);
                                            nextRespSock.writeUTF(msgReceieved);
                                            nextRespSock.flush();
                                            String nextResult = nextReqSock.readUTF();
                                            Log.e(TAG, "Received Response fro *");
                                            StringBuffer resultBuffer = new StringBuffer(nextResult);
                                            for (String[] record : records) {
                                                String key = record[0];
                                                String value = record[2];
                                                resultBuffer.append(key).append("=").append(value);
                                                resultBuffer.append(";");
                                            }
                                            respSock.writeUTF(resultBuffer.toString());
                                        }
                                        break;
                                    default:
                                        String result = "";
                                        boolean hasResult = false;
                                        for (String[] record : records) {
                                            if (record[0].equals(query)) {
                                                hasResult = true;
                                                result = record[2];
                                                break;
                                            }
                                        }
                                        if (hasResult) {
                                            Log.e(TAG, "Writing Result from: " + myPort);
                                            respSock.writeUTF(result);
                                            respSock.flush();
                                        } else {
                                            Log.e(TAG, "Passing Query:" + msgReceieved + "to server :" + next);
                                            Socket nextSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                    Integer.parseInt(next));
                                            DataOutputStream nextRespSock = new DataOutputStream(nextSocket.getOutputStream());
                                            DataInputStream nextReqSock = new DataInputStream(nextSocket.getInputStream());
                                            nextRespSock.writeUTF(msgReceieved);
                                            nextRespSock.flush();
                                            String nextResult = nextReqSock.readUTF();
                                            Log.e(TAG, "Rxd response :" + nextResult);
                                            respSock.writeUTF(nextResult);
                                        }
                                        break;
                                }
                                break;
                            case "DELETE":
                                break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            try {
                for (String port : savedPorts) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));
                    String msgToSend = msgs[0];
                    try (DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {
                        dos.writeUTF(msgToSend);
                        try (DataInputStream dis = new DataInputStream(socket.getInputStream())) {
                            String msg = dis.readUTF();
                            if (msg.equals("ROGER THAT")) {
                                activePorts.add(port);
                                socket.close();
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                e.printStackTrace();
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }

    public MatrixCursor getCursor() {
        String[] headers = new String[]{"key", "value"};
        return new MatrixCursor(headers);
    }

    public void formChord() {
        try {
            for (String each : activePorts) {
                int portNum = getRemotePort(each);
                String hash = genHash(String.valueOf(portNum));
                if (!sortedHashes.contains(hash)) {
                    sortedHashes.add(hash);
                    sortedHashesToPort.put(hash, each);
                }
            }

            Collections.sort(sortedHashes);
            Log.e(TAG, "Ring Order :" + sortedHashes);
            int portNum = getRemotePort(myPort);
            String portNumHash = genHash(String.valueOf(portNum));

            int index = sortedHashes.indexOf(portNumHash);
            myPortHash = portNumHash;

            if (index == 0) {
                prevHash = sortedHashes.get(sortedHashes.size() - 1);
                prev = sortedHashesToPort.get(prevHash);
                nextHash = sortedHashes.get(1);
                next = sortedHashesToPort.get(nextHash);
            } else if (index == sortedHashes.size() - 1) {
                prevHash = sortedHashes.get(sortedHashes.size() - 2);
                prev = sortedHashesToPort.get(prevHash);
                nextHash = sortedHashes.get(0);
                next = sortedHashesToPort.get(nextHash);
            } else {
                prevHash = sortedHashes.get(index - 1);
                prev = sortedHashesToPort.get(prevHash);
                nextHash = sortedHashes.get(index + 1);
                next = sortedHashesToPort.get(nextHash);
            }

        } catch (NoSuchAlgorithmException e) {

        }
    }

    public void setActivePorts() {
        try {
            for (String port : savedPorts) {
                if (port.equals(myPort)) {
                    activePorts.add(myPort);
                    continue;
                }
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port));
                String msgToSend = LIVECHECK;
                try {
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    dos.writeUTF(msgToSend);
                    try {
                        DataInputStream dis = new DataInputStream(socket.getInputStream());
                        String msg = dis.readUTF();
                        if (msg.equals("ROGER THAT")) {
                            activePorts.add(port);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getRemotePort(String remotePort) {
        switch (remotePort) {
            case REMOTE_PORT0:
                return 5554;
            case REMOTE_PORT1:
                return 5556;
            case REMOTE_PORT2:
                return 5558;
            case REMOTE_PORT3:
                return 5560;
            case REMOTE_PORT4:
                return 5562;
            default:
                return 0;
        }
    }


    public String getRemotePort(int remotePort) {
        switch (remotePort) {
            case 5554:
                return REMOTE_PORT0;
            case 5556:
                return REMOTE_PORT1;
            case 5558:
                return REMOTE_PORT2;
            case 5560:
                return REMOTE_PORT3;
            case 5562:
                return REMOTE_PORT4;
            default:
                return "";
        }
    }

    public List<String> declarePorts() {
        List<String> list = new ArrayList<>();
        list.add(REMOTE_PORT0);
        list.add(REMOTE_PORT1);
        list.add(REMOTE_PORT2);
        list.add(REMOTE_PORT3);
        list.add(REMOTE_PORT4);
        return list;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private boolean canInsertWithInMyNode(String recordHash) {

        /**
         * 1. It should either lie with in prevHash AND myPortHash
         * 2. Record hash should be less than all hashes
         * 3. Record hash should be greater than al hashes
         */
        if ((recordHash.compareTo(prevHash) > 0 && recordHash.compareTo(myPortHash) < 0) ||
                (recordHash.compareTo(prevHash) > 0 && recordHash.compareTo(myPortHash) > 0 && myPortHash.compareTo(prevHash) < 0) ||
                (recordHash.compareTo(prevHash) < 0 && recordHash.compareTo(myPortHash) < 0 && myPortHash.compareTo(prevHash) < 0))
            return true;

        return false;

    }

}
