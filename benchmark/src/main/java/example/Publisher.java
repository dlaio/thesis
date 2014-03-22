/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package example;

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

class Publisher {

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
    
    public static class SendThread implements Runnable
    {
    	String user = env("APOLLO_USER", "admin");
        String password = env("APOLLO_PASSWORD", "password");
        String host = env("APOLLO_HOST", "localhost");
        int port = Integer.parseInt(env("APOLLO_PORT", "5672"));
        String channelName = "topic://messages";
        int numMessages = 1000;
        String stringData;
        byte[] bytesData;
        StringBuffer testData = new StringBuffer();
        long sendTime;

		@Override
		public void run() 
		{
	        for (int i=0; i < 10000; i++) 
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
	            
	            System.out.println(String.format("msg id: %d -> send time: %d", i, sendTime));
	          
	            // Send the message
	            try 
	            {
					producer.send(msg);
				} catch (JMSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	            
	            System.out.println(String.format("Sent %d messages", i));
	            
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
    	String user = env("APOLLO_USER", "admin");
        String password = env("APOLLO_PASSWORD", "password");
        String host = env("APOLLO_HOST", "localhost");
        int port = Integer.parseInt(env("APOLLO_PORT", "5672"));
        String channelName = "topic://acknoledgements";
    	BufferedWriter writer;
    	boolean running = true;
    	MessageConsumer consumer = null;
        
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
            if( channelName.startsWith("topic://") ) 
            {
                dest = new TopicImpl(channelName);
            } 
            else 
            {
                dest = new QueueImpl(channelName);
            }

            Connection connection = null;
			try {
				connection = factory.createConnection(user, password);
			} catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
            try {
				connection.start();
			} catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
            Session session = null;
			try {
				session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			} catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
            
			try {
				consumer = session.createConsumer(dest);
			} catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
            long count = 1;
            System.out.println("Waiting for messages...");
            int messageId;
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
                    System.out.println(String.format("Received %d bytes", body.length()));
                }                
                else if( msg instanceof  BytesMessage ) 
                {
                
                	messageId = 0;
                	sentTime = 0;
                	ackTimeSent = 0;
                	ackTimeRecvd = 0;
					try
					{
						messageId = ((BytesMessage) msg).readInt();
						sentTime = ((BytesMessage) msg).readLong();
						ackTimeSent = ((BytesMessage) msg).readLong();
						ackTimeRecvd = System.currentTimeMillis();
					} catch (JMSException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            
                	//String data = msg.getStringProperty("data");
                	System.out.println(String.format("message id: %d - (%d/%d/%d)", messageId, sentTime, ackTimeSent, ackTimeRecvd));
                    try 
                    {
						writer.write(messageId + ",");
						writer.write(sentTime + ",");
						writer.write(ackTimeSent + ",");
						writer.write(ackTimeRecvd + ",");
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
    	
    	
    	ackThread = new Thread(ackProcessor);
    	Thread sendThread = new Thread(new SendThread());
    	
    	BufferedReader commandLine = new java.io.BufferedReader(new InputStreamReader(System.in));

    	
        
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH.mm.ss");  
        File csvFile = new File(df.format(new Date()) +"_Statistics.csv");  
        FileWriter csvOutput = new FileWriter(csvFile);
        BufferedWriter writer = new BufferedWriter( csvOutput );
        writer.write("Message ID,");
        writer.write("Sent time,");
        writer.write("Ack Sent Time,");
        writer.write("Ack Recv Time,");
        writer.newLine();
        
    	ackProcessor.setOuputFile(writer);
        ackThread.start();
        Thread.sleep(1000);
        sendThread.start();


        //producer.send(session.createTextMessage("SHUTDOWN"));
        //Thread.sleep(1000*3);
        // Wait for threads to finish
       
        sendThread.join();
        ackThread.join();
        //ackProcessor.terminate();
        
        // close output files
        writer.flush();
        writer.close();
        csvOutput.close();
        System.exit(0);
    }

    private static String env(String key, String defaultValue) {
        String rc = System.getenv(key);
        if( rc== null )
            return defaultValue;
        return rc;
    }

    private static String arg(String []args, int index, String defaultValue) {
        if( index < args.length )
            return args[index];
        else
            return defaultValue;
    }

}
