/**
 */
package benchmarks;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;

import org.apache.qpid.amqp_1_0.jms.impl.*;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

class Producer {

    public static String readFile(String path) throws IOException
    {
            StringBuilder sb = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new FileReader(path)))
            {
            	String sCurrentLine;
                while ((sCurrentLine = br.readLine()) != null)
                {
                    sb.append(sCurrentLine);
                }

            }

        return sb.toString();
    }
    
    public static class SendProcessor implements Runnable
    {
    	private int clientId = 0;
    	private int numMessages = 0;
        private String stringData;
        private byte[] bytesData;
		private long sendTime;
		private int msgSize;
		ConnectionFactory factory = null; 
        Queue dest = null;
        StringBuffer testData = new StringBuffer();
    	 
        public void setClientId(int id) {
			this.clientId = id;
		}

        public void setFactory(ConnectionFactory factory) {
			this.factory = factory;
		}
        
        public void setDestination(Queue queue) {
        	this.dest = queue;
        }


		public void setNumMessages(int numMessages) {
			this.numMessages = numMessages;
		}
		
		public void setMsgSize(int msgSize) {
			this.msgSize = msgSize;
		}



		@Override
		public void run() 
		{
	        for (int i=0; i < this.msgSize; i++) 
	        {  
	        	testData.append("x"); 
	        }  
	        
	        stringData = testData.toString();   // If you wanted to go char by char
	        bytesData = stringData.getBytes();
	        	

	        Session session = null;
	        Connection connection = null;
	        MessageProducer producer = null;
			try {
				connection = factory.createConnection();
				connection.start();
				session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
				producer = session.createProducer(dest);
				producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
				
			} catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			BytesMessage msg = null;
			try {
				msg = session.createBytesMessage();
			} catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        
	        for( int i=1; i <= numMessages; i ++) 
	        {
	           	//System.out.println(String.format("Press enter to send a message"));
	        	//String s = commandLine.readLine();
   
				// Put data into the output message
	            try 
	            {    
		            //msg.setStringProperty("ebts", DATA);
		            //msg.setIntProperty("number", i);
					//msg.setLongProperty("id", (long)i);
					//sendTime = System.currentTimeMillis();
					//msg.setLongProperty("send_time", sendTime);
					//msg.setStringProperty("data", data);
	            	msg.writeInt(i);
	            	msg.writeInt(clientId);
	            	msg.writeLong(System.currentTimeMillis());
	            	msg.writeBytes(bytesData);
					
				} catch (JMSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	            
	            //System.out.println(String.format("msg id: %d -> send time: %d", i, sendTime));
	          
	            // Send the message
	            try 
	            {
					producer.send(msg);
				} catch (JMSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	            
	            if((i % 100) == 0)
	            {
	            	System.out.println(String.format("Sent %d messages", i));
	            }
	            
	            try {
					msg.clearBody();
				} catch (JMSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
	        }
	        
	        //send shutdown message
        	try {
				msg.writeInt(-1);
				msg.writeInt(clientId);
				msg.writeLong(System.currentTimeMillis());
				producer.send(msg);
			} catch (JMSException e1) 
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
        	
        
       /*
        	// close the connection
	        try {
				//connection.close();
			} catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			*/
		}
    	
    }
    
    public static class AckProcessor implements Runnable
    {
    	BufferedWriter writer;
    	boolean running = true;
    	MessageConsumer consumer = null;
    	ConnectionFactory factory = null;
    	Queue dest = null;

    	
        public void setFactory(ConnectionFactory factory) {
			this.factory = factory;
		}
        
        public void setDestination(Queue queue) {
        	this.dest = queue;
        }


		public void setOuputFile(BufferedWriter writer)
        {
        	this.writer = writer;
        }
        
        public void terminate()
        {
        	
        	running = false;
        	try {
				consumer.close();
			} catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	
        }
        
        @Override
    	public void run()
    	{
    		
            Session session = null;
            Connection connection = null;
            
			try {
				connection = factory.createConnection();
				connection.start();
				session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
				consumer = session.createConsumer(dest);
			} catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
            System.out.println("Waiting for messages...");
            int messageId;
            int clientId;
            int msgType;
            long sentTime;
            long ackTimeSent;
            long ackTimeRecvd;
            while(running == true) 
            {
            	
                Message msg = null;
				try {
					msg = consumer.receive();
				} catch (JMSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

                if( msg instanceof  TextMessage ) 
                {
                    String body = null;
					try {
						body = ((TextMessage) msg).getText();
					} catch (JMSException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                    //System.out.println(String.format("Received %d bytes", body.length()));
                }                
                else if( msg instanceof  BytesMessage ) 
                {
                
                	messageId = 0;
                	clientId = 0;
                	msgType = 0;
                	sentTime = 0;
                	ackTimeSent = 0;
                	ackTimeRecvd = 0;
					try
					{
						messageId = ((BytesMessage) msg).readInt();
						clientId = ((BytesMessage) msg).readInt();
						msgType = ((BytesMessage) msg).readInt();
						sentTime = ((BytesMessage) msg).readLong();
						ackTimeSent = ((BytesMessage) msg).readLong();
						ackTimeRecvd = System.currentTimeMillis();
					} catch (JMSException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					//System.out.println(String.format("message id: %d - (%d/%d/%d)", messageId, sentTime, ackTimeSent, ackTimeRecvd));
                   	try 
                    {
                   		//System.out.println("writing some stuff");
						writer.write(messageId + ",");
						writer.write(clientId + ",");
						writer.write(msgType + ",");
						
						if(msgType == 1)
						{
							writer.write(sentTime + ",");
							writer.write(ackTimeSent + ",");
							writer.write(ackTimeRecvd + ",");
						}
						else
						{
							writer.write(0 + ",");
							writer.write(ackTimeSent + ",");
							writer.write(ackTimeRecvd + ",");
						}
                    	writer.newLine();
                    } catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}


                    if(messageId == -1)
                    {
                    	running = false;
                    }
                
                }
                else 
                {
                    //System.out.println("Unexpected message type: "+msg.getClass());
                }
                
            }
        }
    }
    
    	
    	
    

    public static void main(String []args) throws Exception 
    {
    	AckProcessor ackProcessor = new AckProcessor();
    	Thread ackThread = null;
    	
    	SendProcessor sendProcessor = new SendProcessor();
    	Thread sendThread = null;
    	String brokerType = "unknown";
    	int numMessages = 1000;
    	int msgSize = 1000;
    	Destination msgChannelDest = null;
    	Destination ackChannelDest = null;
    	long startTime = 0;
    	long endTime = 0;
    	
        InitialContext context = null;

        Hashtable<String, String> env = new Hashtable<String, String>(); 
        env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.amqp_1_0.jms.jndi.PropertiesFileInitialContextFactory"); 
        env.put(Context.PROVIDER_URL, "amqp.properties"); 
        try {
			context = new InitialContext(env);
		} catch (NamingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
        
        // Lookup ConnectionFactory and Queue from the context factory
        //ConnectionFactoryImpl factory = (ConnectionFactoryImpl) context.lookup("brokerURI");
        //ConnectionFactory factory = = (ConnectionFactory) context.lookup("SBCF");
        //msgChannelDest = (Destination) context.lookup("MSGS");
        //ackChannelDest = (Destination) context.lookup("ACKS");
        
        ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("brokerURI");
        //connection = connectionFactory.createConnection();
        Queue msgQueue = (Queue) context.lookup("MSGS");
        Queue ackQueue = (Queue) context.lookup("ACKS");
    	
  
    	// Read command line args
    	// broker type, host, port, username, password, send channel name, ack channel name, numMessages, use persistence 
    	if (args.length != 3)
    	{
    	     System.out.println("Usage: Producer brokerType numMessages msgSize");
    	     System.exit(-1);
    	}
    	else
    	{
	    	brokerType = args[0];
	    	numMessages = Integer.parseInt(args[1]);
	    	msgSize = Integer.parseInt(args[2]);
    	}
    	
    	
    	// setup producer
    	sendProcessor.setFactory(connectionFactory);
    	sendProcessor.setDestination(msgQueue);
    	sendProcessor.setNumMessages(numMessages);
    	sendProcessor.setMsgSize(msgSize);
    	
    	ackProcessor.setFactory(connectionFactory);
    	ackProcessor.setDestination(ackQueue);
        
    	// Create output file
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH.mm.ss");  
        File csvFile = new File(brokerType + "_" + numMessages + "_" + msgSize + "_" + df.format(new Date()) +".csv");  
        FileWriter csvOutput = new FileWriter(csvFile);
        BufferedWriter writer = new BufferedWriter( csvOutput );
        writer.write("Message ID,");
        writer.write("Client ID,");
        writer.write("Result,");
        writer.write("Sent time,");
        writer.write("Ack Sent Time,");
        writer.write("Ack Recv Time,");
        writer.newLine();
        
        // set output file in ackProcessor
    	ackProcessor.setOuputFile(writer);
    	
    	// Create processing threads
    	ackThread = new Thread(ackProcessor);
    	sendThread = new Thread(sendProcessor);
        
    	ackThread.start();
    	// TODO - replace with a function that returns when ack thread has started and connected
        Thread.sleep(2000);
        
        // start the message producer
        startTime = System.currentTimeMillis();
        sendThread.start();


        // wait for threads to finish
        sendThread.join();
        ackThread.join();
        endTime = System.currentTimeMillis();
        
        System.out.printf("Start time: %d\n", startTime);
        System.out.printf("End time: %d\n", endTime);
        float timeDeltaSec = ((float)(endTime - startTime)/1000);
        float throughput = numMessages / timeDeltaSec;
        float bandwidth = ((float)(numMessages * msgSize) / timeDeltaSec/1000000);
        
        System.out.printf("runtime: %f sec\n", timeDeltaSec);
        System.out.printf("throughput: %f messages/sec\n", throughput);
        System.out.printf("bandwidth: %f MB/sec\n", bandwidth);
        
        writer.newLine();
        writer.write("runtime," + timeDeltaSec + ",sec");
        writer.newLine();
        writer.write("throughput," + throughput + ",msgs/sec");
        writer.newLine();
        writer.write("bandwidth," + bandwidth + ",MB/sec");
        writer.newLine();
        
        
        // close output files
        writer.flush();
        writer.close();
        csvOutput.close();
        System.exit(0);
    }

}
