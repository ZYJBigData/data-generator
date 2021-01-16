package com.bizseer.bigdata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

/**
 * @author zhangyingjie
 */
public class SocketClientSink {

    public static void main(String[] args) throws Exception {
        //1. 构造ServerSocket实例，指定服务端口。
        ServerSocket servSock = new ServerSocket();
        servSock.bind(new InetSocketAddress("192.168.1.6", 8400));
        System.out.println("socket服务端:" + servSock.getInetAddress() + ":" + servSock.getLocalPort() + "");

        while (true) {
            // 2.调用accept方法，建立和客户端的连接
            Socket client = servSock.accept();
            SocketAddress clientAddress = client.getRemoteSocketAddress();
            System.out.println("socket链接客户端:" + clientAddress);
            new Thread(new SSocket(client)).start();
        }
    }
}

//服务器进程
class SSocket implements Runnable {
    Socket client;
    public SSocket(Socket client) {
        this.client = client;
    }
    @Override
    public void run() {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(client.getInputStream()));
            String msg;
            //如果输入流不为空,将接受到的信息打印到相应的文本框中并反馈回收到的信息
            if ((msg = br.readLine()) != null) {
                System.out.println(msg);
            }
            System.out.println(msg);
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
