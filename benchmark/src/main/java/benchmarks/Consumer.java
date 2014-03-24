/**
 */
package benchmarks;

import java.util.Hashtable;

import org.apache.qpid.amqp_1_0.jms.impl.*;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

class Consumer {
	


	/*
	public void sendNakMsg(int messageId)
	{
        // send response message
		ack.writeInt(messageId);
		// message type
		ack.writeLong(sentTime);
		ack.writeLong(System.currentTimeMillis());

        try 
        {
        	ackProducer.send(ack);
		} catch (JMSException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
        ack.clearBody();
	}
	*/
	
	/*
    public static class MsgProcessor implements Runnable
    {
    
    }
    */

    public static void main(String []args) throws JMSException, NamingException {

        boolean running = true;
    	String user = "admin";
    	String password = "password";
    	String host = "localhost";
    	int port = 5672;
    	String msgChannelName = "topic://messages";
    	String ackChannelName = "topic://acknoledgements";
    	int nextExpectedMsgId = 1;
    	int messageId = 0;
    	long sentTime = 0;
    	int msgSize = 0;
    	
    	ConnectionFactoryImpl factory;
        Destination msgChannelDest = null;
        Destination ackChannelDest = null;
        Connection connection;
        Session session;
        
        Connection connection2;
        Session session2;
        
        MessageConsumer consumer;
        MessageProducer ackProducer;
        
        InitialContext context = null;

		
        
        Hashtable<String, String> env = new Hashtable<String, String>(); 
        env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.amqp_1_0.jms.jndi.PropertiesFileInitialContextFactory"); 
        env.put(Context.PROVIDER_URL, "test.properties"); 
        try {
			context = new InitialContext(env);
		} catch (NamingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
        
        // Lookup ConnectionFactory and Queue
        ConnectionFactory cf = (ConnectionFactory) context.lookup("SBCF");
        msgChannelDest = (Destination) context.lookup("MSGS");
        ackChannelDest = (Destination) context.lookup("ACKS");
    
    	// Read command line args
    	// host, port, username, password, msgChannelName ackChannelName
    	if (args.length != 6)
    	{
    	     System.out.println("Usage: Consumer host port username password msgChannelName ackChannelName");
    	     System.exit(-1);
    	}
    	else
    	{
	    	host = args[0];
	    	port = Integer.parseInt(args[1]);
	    	user = args[2];
	    	password = args[3];
	    	//msgChannelName = args[4];
	    	//ackChannelName = args[5];
    	}

    	/*
        factory = new ConnectionFactoryImpl(host, port, user, password);

        if( msgChannelName.startsWith("topic://") ) 
        {
        	msgChannelDest = new TopicImpl(msgChannelName);
        } else 
        {
        	msgChannelDest = new QueueImpl(msgChannelName);
        }
        
        
        
        if( ackChannelName.startsWith("topic://") ) 
        {
        	ackChannelDest = new TopicImpl(ackChannelName);
        } else 
        {
        	ackChannelDest = new QueueImpl(ackChannelName);
        }
        */

        // Create Connection
        connection = cf.createConnection();
        //connection = factory.createConnection(user, password);
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        //connection2 = factory.createConnection(user, password);
        connection2 = cf.createConnection();
        connection2.start();
        session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        // Create message consumer
        consumer = session.createConsumer(msgChannelDest);
        // Create message ack
        ackProducer = session2.createProducer(ackChannelDest);
        
        
        BytesMessage ack = null;
		try 
		{
			ack = session.createBytesMessage();
		} catch (JMSException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        long start = System.currentTimeMillis();
        nextExpectedMsgId = 1;
        System.out.println("Waiting for messages...");
        while(running == true) 
        {
        	//System.out.println("calling receive...");
            Message msg = consumer.receive();
            //System.out.println("returned from receive...");
            if( msg instanceof  TextMessage ) 
            {
            	/*
                String body = ((TextMessage) msg).getText();
                System.out.println(String.format("Received %d bytes", body.length()));
                if( "SHUTDOWN".equals(body)) 
                {
                    long diff = System.currentTimeMillis() - start;
                    System.out.println(String.format("Received %d in %.2f seconds", count, (1.0*diff/1000.0)));
                    connection.close();
                    System.exit(1);
                } 
                else 
                {                	
                    count ++;
                }
                */
            } 
            else if( msg instanceof  BytesMessage ) 
            {
            	msgSize = (int) ((BytesMessage) msg).getBodyLength();
            	messageId = ((BytesMessage) msg).readInt();
            	sentTime = ((BytesMessage) msg).readLong();
            	
            	if(nextExpectedMsgId != messageId)
            	{
            		int numDroppedMessages = messageId - nextExpectedMsgId;
            		System.out.println(String.format("Dropped %d messages", numDroppedMessages));
            		// TODO - send dropped message NAK
            		for(int nakMsg = 0; nakMsg < numDroppedMessages; nakMsg++)
            		{
            			//sendNakMsg();
                        // send response message
            			ack.writeInt(messageId + nakMsg);
            			// message type
            			ack.writeInt(-1);
            			ack.writeLong(sentTime);
            			ack.writeLong(System.currentTimeMillis());

                        try 
                        {
                        	ackProducer.send(ack);
            			} catch (JMSException e) 
            			{
            				// TODO Auto-generated catch block
            				e.printStackTrace();
            			}
            			
                        ack.clearBody();
            		}
            	}
            	
            	if((messageId % 50) ==0)
            	{
            		System.out.println(String.format("message id: %d - sent at: %d (%d bytes)", messageId, sentTime, msgSize));
            	}
            	
        		nextExpectedMsgId = messageId + 1;
            }
            else 
            {
                System.out.println("Unexpected message type: "+msg.getClass());
            }
            
            
            // send response message
			ack.writeInt(messageId);
			// message type
			ack.writeInt(1);
			ack.writeLong(sentTime);
			ack.writeLong(System.currentTimeMillis());

            try 
            {
            	ackProducer.send(ack);
			} catch (JMSException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
            ack.clearBody();
            
            if(messageId == -1)
            {
            	running = false;
            }
            
        }
        
        ackProducer.close();
        consumer.close();
        System.exit(0);
    }

}
