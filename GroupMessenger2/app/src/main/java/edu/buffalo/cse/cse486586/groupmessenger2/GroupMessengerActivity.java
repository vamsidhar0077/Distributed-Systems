
package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();

    public static Uri URI = null;
    static final int SERVER_PORT = 10000;
    static final String SCHEME = "content";

    PriorityQueue<Client> pQueue = new PriorityQueue<>(1000, new ClientComparator());
    static Set<Integer> uniqIds = new HashSet<>();
    static final String AUTHORITY = "edu.buffalo.cse.cse486586.groupmessenger2.provider";

    //Defining Constants
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final String DELIMITER = ";";
    static String failedPort = "";
    static int sequenceNum = 0;
    String sendPort = "";
    static AtomicInteger counter = new AtomicInteger(0);
    String[] savedPorts = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        URI = constructURI();

        //HardCode Port Numbers
//        savedPorts = declarePorts();

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {

        }

        final TextView et = (TextView) findViewById(R.id.editText1);

        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String msg = et.getText().toString() + "\n";
                et.setText("");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    public Uri constructURI() {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(AUTHORITY);
        uriBuilder.scheme(SCHEME);
        return uriBuilder.build();
    }

//    public List<String> declarePorts() {
//        List<String> ports = new ArrayList<>();
//        ports.add(REMOTE_PORT0);
//        ports.add(REMOTE_PORT1);
//        ports.add(REMOTE_PORT2);
//        ports.add(REMOTE_PORT3);
//        ports.add(REMOTE_PORT4);
//        return ports;
//    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
//                    try{
                    DataInputStream incoming = new DataInputStream(socket.getInputStream());
                    String message = incoming.readUTF();
                    Client client = mapMessageToClient(message);
                    sendPort = client.getSendingPort();
                    counter.getAndIncrement();
                    client.setProposalNum(counter.get());
                    pQueue.add(client);
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    dos.writeUTF(Integer.toString(counter.get()));
//                    dos.flush();
//
//                    }   catch (Exception e){
//                        e.printStackTrace();
//                    }

                    while (true) {
                        try {
                            String message2 = incoming.readUTF();
                            if (message2.startsWith("Failed:")) {
                                failedPort = message2.replaceAll("Final:", "");
                                pQueue.remove(client);
//                                checkForFailures();
                            } else {
                                Client temp = mapMessageToClient(message2);
                                pQueue.remove(client);
                                temp.isFinal = true;
                                pQueue.add(temp);
                            }
//                        Log.e(TAG, pQueue.toString()+"--->Publishing Before");
//
//                        Client client1 = mapMessageToClient(message);
//                        Log.e(TAG, client.toString());
//                        if (counter.get() <= client.getProposalNumber()){
//                            counter.set(client.getProposalNumber() + 1);
//                            client.setFinal(true);
//                        }
////                        Log.e(TAG, pQueue.toString());
//                        pQueue.remove(client);
//                        pQueue.add(client);
                            while (pQueue.size() > 0 && pQueue.peek().isFinal) {
                                Client top = pQueue.poll();
                                publishProgress(top.msg);
                            }
                            break;
                        } catch (Exception e) {
                            pQueue.remove(client);
//                            e.printStackTrace();
                            break;
                        }
                    }

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
//        private void checkForFailures() {
//            PriorityQueue<Client> local = new PriorityQueue<>(1000, new ClientComparator());
//            while (!pQueue.isEmpty()) {
//                Client each = pQueue.poll();
//                if (each.portNum.equals(failedPort) && !each.isFinal) {
////                                                    pq.remove(each);
//                } else {
//                    local.add(each);
//                }
//            }
//            pQueue.addAll(local);
//            while (pQueue.size() > 0 && pQueue.peek().isFinal) {
//                Client top = pQueue.poll();
//                publishProgress(top.msg);
//            }
//        }

        protected void onProgressUpdate(String... strings) {
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");

            //Defining Content Values
            ContentValues messageValues = new ContentValues();
            messageValues.put("key", String.valueOf(sequenceNum++));
            messageValues.put("value", strReceived);

            //Writing the content to a file
            getContentResolver().insert(URI, messageValues);
            return;
        }

        private void checkForFailures() {
            Log.e(TAG, "Publishing");
            while (pQueue.size() > 0 && pQueue.peek().isFinal) {
                Client top = pQueue.poll();
                publishProgress(top.msg);
            }
        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                int proposalNum = 0;
                String message = msgs[0];
                String sendingPort = msgs[1];
                int max = Integer.MIN_VALUE;
                int uniqueId = getUniqueId();

                Socket[] sockets = new Socket[5];
                int local = 0;
                for (String port : savedPorts) {
                    sockets[local] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));

                    Client client = new Client(uniqueId, message, port, proposalNum, sendingPort);
                    client.failurePort = failedPort;
                    try {
                        DataOutputStream dos = new DataOutputStream(sockets[local].getOutputStream());
                        DataInputStream dis = new DataInputStream(sockets[local++].getInputStream());
                        dos.writeUTF(client.toString());
                        dos.flush();
                        int proposed = Integer.parseInt(dis.readUTF());
                        max = Math.max(proposed, max);
//                        dos.close();
                    } catch (IOException | NullPointerException e) {
                        failedPort = port;
//                        for (String port : savedPorts) {
//                            Client client = new Client(uniqueId, message, port, max, sendingPort);
//                            try {
//                                DataOutputStream dos = new DataOutputStream(sockets[local++].getOutputStream());
////                        DataInputStream dis = new DataInputStream(sockets[local++].getInputStream());
//                                client.setFinal(true);
//                                client.failurePort = failedPort;
//                                Log.i(TAG, "C4: Sending failure notice: "+failedPort);
//                                dos.writeUTF("Failed:" + failedPort);
//                                dos.flush();
//                            } catch (IOException | NullPointerException e) {
//                                e.printStackTrace();
//                            }
//                        }
//                        e.printStackTrace();
                    }
                }
                local = 0;
                for (String port : savedPorts) {
                    Client client = new Client(uniqueId, message, port, max, sendingPort);
                    try {
                        DataOutputStream dos = new DataOutputStream(sockets[local++].getOutputStream());
                        client.failurePort = failedPort;
                        Log.i(TAG, "C4: Sending agreement:" + client.toString());
                        dos.writeUTF(client.toString());
                        dos.flush();
                    } catch (IOException | NullPointerException e) {
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


    public static int getUniqueId() {
        int rand = new Random().nextInt();
        while (uniqIds.contains(rand)) {
            rand = new Random().nextInt();
        }
        uniqIds.add(rand);
        return rand;
    }

    public Client mapMessageToClient(String message) {
        String[] parts = message.split(";");
        int id = Integer.parseInt(parts[0]);
        String msg = parts[1];
        String portNum = parts[2];
        int proposalNum = Integer.parseInt(parts[3]);
        boolean isFinal = Boolean.parseBoolean(parts[4]);
        String sendingPort = parts[5];

        Client client = new Client(id, msg, portNum, proposalNum, sendingPort);
        client.setFinal(isFinal);
        return client;
    }


    class Client {

        int uniqueId;
        String msg;
        String portNum;
        int proposalNum;
        boolean isFinal;
        String sendingPort;
        String failurePort = failedPort;

        Client(int uniqueId, String msg, String portNum, int proposalNum, String sendingPort) {
            this.uniqueId = uniqueId;
            this.msg = msg;
            this.portNum = portNum;
            this.proposalNum = proposalNum;
            this.isFinal = false;
            this.sendingPort = sendingPort;
        }

        public void setProposalNum(int proposalNum) {
            this.proposalNum = proposalNum;
        }

        public void setFinal(boolean decision) {
            this.isFinal = decision;
        }

        public int getProposalNumber() {
            return this.proposalNum;
        }

        public String getSendingPort() {
            return this.sendingPort;
        }

        public boolean getFinal() {
            return this.isFinal;
        }

        public String getMsg() {
            return this.msg;
        }

        @Override
        public String toString() {
            return this.uniqueId + DELIMITER + this.msg + DELIMITER + this.portNum + DELIMITER +
                    this.proposalNum + DELIMITER + this.isFinal + DELIMITER + this.sendingPort + DELIMITER + this.failurePort;
        }

        @Override
        public boolean equals(Object client) {
            return this.uniqueId == ((Client) client).uniqueId;
        }

    }

    class ClientComparator implements Comparator<Client> {
        @Override
        public int compare(Client c1, Client c2) {
            if (c1.proposalNum == c2.proposalNum) {
                if (getId(c1.portNum) < getId(c2.portNum)) {
                    return -1;
                } else if (getId(c1.portNum) > getId(c2.portNum)) {
                    return 1;
                } else {
                    return c1.uniqueId - c2.uniqueId;
                }
            } else {
                return c1.proposalNum - c2.proposalNum;
            }
        }
    }

    public int getId(String portNum) {
        switch (portNum) {
            case "11108":
                return 1;
            case "11112":
                return 2;
            case "11116":
                return 3;
            case "11120":
                return 4;
            case "11124":
                return 5;
            default:
                return Integer.MAX_VALUE;
        }
    }

}

