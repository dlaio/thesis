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

import org.apache.qpid.amqp_1_0.jms.impl.*;

import javax.jms.*;

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
    	private String user;
    	private String password;
    	private String host;
    	private int port;
    	private String channelName;
    	private int numMessages;
        private String stringData;
        private byte[] bytesData;
		private long sendTime;
		private int msgSize;
        StringBuffer testData = new StringBuffer();
    	        

		public void setUser(String user) {
			this.user = user;
		}


		public void setPassword(String password) {
			this.password = password;
		}


		public void setHost(String host) {
			this.host = host;
		}


		public void setPort(int port) {
			this.port = port;
		}


		public void setChannelName(String channelName) {
			this.channelName = channelName;
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
	        	
	        
			ConnectionFactoryImpl factory = new ConnectionFactoryImpl(host, port, user, password);
	        Destination dest = null;
	        if( channelName.startsWith("topic://") ) 
	        {
	            dest = new TopicImpl(channelName);
	        } else 
	        {
	            dest = new QueueImpl(channelName);
	        }

	        Session session = null;
	        Connection connection = null;
	        MessageProducer producer = null;
			try {
				connection = factory.createConnection(user, password);
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
				msg.writeLong(System.currentTimeMillis());
				producer.send(msg);
			} catch (JMSException e1) 
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
        	
        
        	// close the connection
	        try {
				connection.close();
			} catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
    	
    }
    
    public static class AckProcessor implements Runnable
    {
    	/*
    	String user = env("APOLLO_USER", "admin");
        String password = env("APOLLO_PASSWORD", "password");
        String host = env("APOLLO_HOST", "localhost");
        int port = Integer.parseInt(env("APOLLO_PORT", "5672"));
        String channelName = "topic://acknoledgements";
    	*/
    	private String user;
    	private String password;
    	private String host;
    	private int port;
    	private String channelName;
    	BufferedWriter writer;
    	boolean running = true;
    	MessageConsumer consumer = null;

    	
        public void setUser(String user) {
			this.user = user;
		}

		public void setPassword(String password) {
			this.password = password;
		}

		public void setHost(String host) {
			this.host = host;
		}

		public void setPort(int port) {
			this.port = port;
		}

		public void setChannelName(String channelName) {
			this.channelName = channelName;
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
    		ConnectionFactoryImpl factory = new ConnectionFactoryImpl(host, port, user, password);
            Destination dest = null;
            Session session = null;
            Connection connection = null;
            if( channelName.startsWith("topic://") ) 
            {
                dest = new TopicImpl(channelName);
            } 
            else 
            {
                dest = new QueueImpl(channelName);
            }

            
			try {
				connection = factory.createConnection(user, password);
				connection.start();
				session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
				consumer = session.createConsumer(dest);
			} catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
            System.out.println("Waiting for messages...");
            int messageId;
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
                	msgType = 0;
                	sentTime = 0;
                	ackTimeSent = 0;
                	ackTimeRecvd = 0;
					try
					{
						messageId = ((BytesMessage) msg).readInt();
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
						writer.write(messageId + ",");
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
    	String user = "admin";
    	String password = "password";
    	String host = "localhost";
    	int port = 5672;
    	String msgChannelName = "topic://messages";
    	String ackChannelName = "topic://acknoledgements";
    	int numMessages = 1000;
    	int msgSize = 1000;
    	
  
    	// Read command line args
    	// broker type, host, port, username, password, send channel name, ack channel name, numMessages, use persistence 
    	if (args.length != 9)
    	{
    	     System.out.println("Usage: Producer brokerType host port username password msgChannelName ackChannelName numMessages msgSize");
    	     System.exit(-1);
    	}
    	else
    	{
	    	brokerType = args[0];
	    	host = args[1];
	    	port = Integer.parseInt(args[2]);
	    	user = args[3];
	    	password = args[4];
	    	msgChannelName = args[5];
	    	ackChannelName = args[6];
	    	numMessages = Integer.parseInt(args[7]);
	    	msgSize = Integer.parseInt(args[8]);
    	}
    	
    	
    	/*
    	 * APOLLO SETTINGS
    	user = "admin"
    	password = "password"
    	host = "localhost"
    	port = 5672
    	channelName = topic://messages
        */
    	// setup producer
    	sendProcessor.setUser(user);
    	sendProcessor.setPassword(password);
    	sendProcessor.setHost(host);
    	sendProcessor.setPort(port);
    	sendProcessor.setChannelName(msgChannelName);
    	sendProcessor.setNumMessages(numMessages);
    	sendProcessor.setMsgSize(msgSize);
    	
    	ackProcessor.setUser(user);
    	ackProcessor.setPassword(password);
    	ackProcessor.setHost(host);
    	ackProcessor.setPort(port);
    	ackProcessor.setChannelName(ackChannelName);
        
    	// Create output file
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH.mm.ss");  
        File csvFile = new File(brokerType + "_" + numMessages + "_" + msgSize + "_" + df.format(new Date()) +".csv");  
        FileWriter csvOutput = new FileWriter(csvFile);
        BufferedWriter writer = new BufferedWriter( csvOutput );
        writer.write("Message ID,");
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
        sendThread.start();


        // wait for threads to finish
        sendThread.join();
        ackThread.join();
        
        // close output files
        writer.flush();
        writer.close();
        csvOutput.close();
        System.exit(0);
    }

}
