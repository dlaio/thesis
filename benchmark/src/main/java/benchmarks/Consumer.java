/**
 */
package benchmarks;

import org.apache.qpid.amqp_1_0.jms.impl.*;

import javax.jms.*;

class Consumer {

    public static void main(String []args) throws JMSException {

        boolean running = true;
    	String user = "admin";
    	String password = "password";
    	String host = "localhost";
    	int port = 5672;
    	String msgChannelName = "topic://messages";
    	String ackChannelName = "topic://acknoledgements";

    
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
	    	msgChannelName = args[4];
	    	ackChannelName = args[5];
    	}

        ConnectionFactoryImpl factory = new ConnectionFactoryImpl(host, port, user, password);
        Destination msgChannelDest = null;
        if( msgChannelName.startsWith("topic://") ) 
        {
        	msgChannelDest = new TopicImpl(msgChannelName);
        } else 
        {
        	msgChannelDest = new QueueImpl(msgChannelName);
        }
        
        
        Destination ackChannelDest = null;
        if( ackChannelName.startsWith("topic://") ) 
        {
        	ackChannelDest = new TopicImpl(ackChannelName);
        } else 
        {
        	ackChannelDest = new QueueImpl(ackChannelName);
        }

        Connection connection = factory.createConnection(user, password);
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        Connection connection2 = factory.createConnection(user, password);
        connection2.start();
        Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        // Create message consumer
        MessageConsumer consumer = session.createConsumer(msgChannelDest);
        // Create message ack
        MessageProducer ackProducer = session2.createProducer(ackChannelDest);
        
        
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
        long count = 1;
        System.out.println("Waiting for messages...");
        while(running == true) 
        {
        	int messageId = 0;
        	long sentTime = 0;
        	//System.out.println("calling receive...");
            Message msg = consumer.receive();
            //System.out.println("returned from receive...");
            if( msg instanceof  TextMessage ) 
            {
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
                	/*
                	int msgSize = 0;
                	//(int) ((TextMessage) msg).getBodyLength();
                	
                	try
                	{
                		messageId = msg.getLongProperty("id");
                	} catch (NumberFormatException ignore) 
                    {
                    }
                	
                	try
                	{
                		sentTime = msg.getLongProperty("send_time");
                	}catch (NumberFormatException ignore) 
                    {
                    }
                	
                	try
                	{
                		String data = msg.getStringProperty("data");
                	}
                	catch (NumberFormatException ignore) 
                    {
                    }
                	
                	System.out.println(String.format("message id: %d - sent at: %d (%d bytes)", messageId, sentTime, msgSize));
                
                	
                    try 
                    {
                        if( count != msg.getIntProperty("id") ) 
                        {
                            System.out.println("mismatch: "+count+"!="+msg.getIntProperty("id"));
                        }
                    } catch (NumberFormatException ignore) 
                    {
                    }
                    if( count == 1 ) 
                    {
                        start = System.currentTimeMillis();
                    } 
                    
                    System.out.println(String.format("Received %d messages.", count));
                    */
                	
                    count ++;
                }

            } 
            else if( msg instanceof  BytesMessage ) 
            {
            	int msgSize = (int) ((BytesMessage) msg).getBodyLength();
            	messageId = ((BytesMessage) msg).readInt();
            	sentTime = ((BytesMessage) msg).readLong();
            	
            	if((messageId % 50) ==0)
            	{
            		System.out.println(String.format("message id: %d - sent at: %d (%d bytes)", messageId, sentTime, msgSize));
            	}
            }
            else 
            {
                System.out.println("Unexpected message type: "+msg.getClass());
            }
            
            
            // send response message
			ack.writeInt(messageId);
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
