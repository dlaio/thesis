/**
 */
package benchmarks;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;

import org.apache.qpid.amqp_1_0.jms.impl.*;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

class Consumer {
	
	static final Logger logger = Logger.getLogger(Consumer.class.getName());

    public static void main(String []args) throws JMSException, NamingException {

        boolean running = true;
    	int clientId = 0;
    	int nextExpectedMsgId = 1;
    	int messageId = 0;
    	int producerId = 0;
    	long sentTime = 0;
    	int msgSize = 0;
    	int messagesRecvd = 0;
    	
    	logger.trace("Entering application.");
    	
        Destination msgChannelDest = null;
        Destination ackChannelDest = null;
        Connection connection;
        Session consumerSession;
        Session producerSession;
        String contextFileName = null;
        
        MessageConsumer consumer;
        MessageProducer ackProducer;
        
        String brokerType = null;
        
        InitialContext context = null;

        Hashtable<String, String> env = new Hashtable<String, String>(); 
        env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.amqp_1_0.jms.jndi.PropertiesFileInitialContextFactory"); 
       
          
    	// Read command line args
    	if(args.length == 1)
    	{
    		contextFileName = args[0];
    	}
    	else
    	{
    	     System.out.println("Usage: Consumer contextFileName");
    	     System.exit(-1);
    	}
    	
		logger.debug("context file name is: " + contextFileName);
		env.put(Context.PROVIDER_URL, contextFileName); 
		  
		try 
		{
			context = new InitialContext(env);
		} catch (NamingException e1)
		{
			e1.printStackTrace();
		} 
		
		Properties properties = new Properties();
		try {
		  properties.load(new FileInputStream(contextFileName));
		} catch (IOException e) 
		{
			e.printStackTrace();
		}
          
		// Lookup ConnectionFactory and Queue from the context factory
		ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("brokerURI");
		connection = connectionFactory.createConnection();
		Queue msgQueue = (Queue) context.lookup("MSGS");
		Queue ackQueue = (Queue) context.lookup("ACKS");
				
		clientId = Integer.parseInt(properties.getProperty("clientId"));
		brokerType = properties.getProperty("brokerType");
		
		logger.debug("clientId is: " + clientId);
		
    	//session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    	producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    	consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

    	connection.start();
    	
    	//APOLLO
        if(brokerType.equalsIgnoreCase("APOLLO"))
        {
        	ackChannelDest = new QueueImpl("queue://acks");
        	msgChannelDest = new QueueImpl("queue://msgs");
        	
        	consumer = consumerSession.createConsumer(msgChannelDest, null);
        	ackProducer = producerSession.createProducer(ackChannelDest);
        }
        //HORNETQ
        else if(brokerType.equalsIgnoreCase("HORNETQ"))
        {
        	// Create message consumer
        	consumer = consumerSession.createConsumer(msgQueue, "color = red");
        	// Create message ack
        	ackProducer = producerSession.createProducer(ackQueue);
        	
        }
        else
        {
        	// Create message consumer
        	consumer = consumerSession.createConsumer(msgQueue);
        	// Create message ack
        	ackProducer = producerSession.createProducer(ackQueue);
        }

        BytesMessage ack = null;
		try 
		{
			ack = producerSession.createBytesMessage();
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
            messagesRecvd++;
            //System.out.println("returned from receive...");
            if( msg instanceof  BytesMessage ) 
            {
            	msgSize = (int) ((BytesMessage) msg).getBodyLength();
            	messageId = ((BytesMessage) msg).readInt();
            	producerId = ((BytesMessage) msg).readInt();
            	sentTime = ((BytesMessage) msg).readLong();
            	
            	if(nextExpectedMsgId != messageId)
            	{
            		int numDroppedMessages = messageId - nextExpectedMsgId;
            		System.out.println(String.format("Dropped %d messages (expected: %d | received: %d)", numDroppedMessages, nextExpectedMsgId, messageId));
            	}
            		
                // send response message
    			ack.writeInt(messageId);
    			ack.writeInt(producerId);
    			ack.writeInt(clientId);
    			// message type
    			ack.writeInt(1);
    			ack.writeLong(sentTime);
    			ack.writeLong(System.currentTimeMillis());

                try 
                {
                	//System.out.println("sending ack");
                	ackProducer.send(ack);
    			} catch (JMSException e) 
    			{
    				e.printStackTrace();
    			}
    			
                ack.clearBody();
            	
            	
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
            
                        
            if(messageId == -1)
            {
            	running = false;
            	System.out.println(String.format("Received %d messages", messagesRecvd));
            }
            
        }
        
        consumer.close();
        System.exit(0);
    }

}
