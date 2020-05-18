package edu.buffalo.cse.cse486586.groupmessenger1;

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

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 */
public class GroupMessengerActivity extends Activity {


    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    public static Uri URI = null;
    static final int SERVER_PORT = 10000;
    static int sequenceNum = 0;
    static final String SCHEME = "content";
    static final String AUTHORITY = "edu.buffalo.cse.cse486586.groupmessenger1.provider";
    static String[] savedPorts = null;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        //Building the URI with the given scheme & authority
        URI = constructURI();

        //HardCode Port Numbers
        savedPorts = declarePorts();

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

    public Uri constructURI() {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(AUTHORITY);
        uriBuilder.scheme(SCHEME);
        return uriBuilder.build();
    }

    public String[] declarePorts() {
        return new String[]{REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                //Referrence:  http://developer.android.com/reference/android/os/AsyncTask.html
                while (true) {
                    Socket socket = serverSocket.accept();
                    try (DataInputStream dis = new DataInputStream(socket.getInputStream())) {
                        String msgReceieved = dis.readUTF();
                        publishProgress(msgReceieved);
                        try (DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {
                            dos.writeUTF("ROGER THAT");
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
                            if (msg.equals("ROGER THAT")) socket.close();
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
}
