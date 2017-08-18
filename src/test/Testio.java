package test;
/*
import io.netty.channel.nio.NioEventLoopGroup;
import msg.Challenge;
import msg.Response;
import msg._Messages_;
import perfect.io.*;

import java.util.Arrays;


public class Testio {
    public static void main(String[] args) throws InterruptedException {

        NioEventLoopGroup boss = new NioEventLoopGroup();
        NioEventLoopGroup work = new NioEventLoopGroup();

        Server.Conf sc = new Server.Conf();
        sc.bossGroup = boss;
        sc.workGroup = work;
        sc.port = 11218;
        sc.stubs = _Messages_.msgs;
        sc.dispatcher = Dispatcher.defaultDispatcher();

        Client.Conf cc = new Client.Conf();
        cc.bossGroup = boss;
        cc.ip = "127.0.0.1";
        cc.port = 11218;
        cc.stubs = _Messages_.msgs;
        cc.dispatcher = Dispatcher.defaultDispatcher();


        Challenge.handler = (m) -> {
            Connection s = (Connection)m.getContext();
            Response res = new Response();
            res.user = m.user;


            s.send(res);
        };
        Response.handler = (msg) -> {

        };

        Server server = new Server<Session>(sc) {
            @Override
            protected Session newSession(Connection conn) {
                return new Session(conn);
            }

            @Override
            protected void onAddSession(Session conn) {

                Challenge c = new Challenge();
                c.nonce = Long.toString(System.currentTimeMillis());
                msg.User u = c.user;
                u.a = 12;
                u.a2 = true;
                u.a3 = 12345678912345678L;
                u.a4 = 1218.25f;
                u.a5 = 111111222222.33333;
                u.a6 = "huangqiang";
                u.a7 = new byte[]{1,2,3,5,-1};
                u.a8.addAll(Arrays.asList(1,3,5));

                msg.Role r = new msg.Role();
                r.a = 18;
                r.a2 = true;
                u.a9.add(r);

                msg.Test1 t = new msg.Test2();
                t.a = 112233;
                u.a19.add(t);

                u.b1.addAll(Arrays.asList(2, 4, 6));

                u.b2.put(5, 100L);
                u.b3.put(10, r);
                u.b4.put(20, t);
                u.c1.a = 446677;

                msg.Test3 x = new msg.Test3();
                x.a3 = -15;
                u.c2 = x;
                conn.send(c);
            }

            @Override
            protected void onDelSession(Session conn) {
                System.out.println("Server.onDelSession " + conn);
            }
        };

        server.open();

        Client client = new Client(cc) {
            @Override
            protected Session newSession(Connection conn) {
                return new Session(conn);
            }

            @Override
            protected void onAddSession(Session session) {

            }

            @Override
            protected void onDelSession(Session session) {

            }
        };
        client.open();


        Thread.sleep(3000);
        client.close();

        Thread.sleep(1000);
        System.exit(0);

    }
}
*/
