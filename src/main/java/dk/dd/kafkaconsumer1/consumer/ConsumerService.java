package dk.dd.kafkaconsumer1.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class ConsumerService
{
      private int countDown = 0;
      private boolean initiated = false;
      // get logger for my class
      private static final Logger logger = LoggerFactory.getLogger(ConsumerService.class);
      
      @KafkaListener(topics = "message-topic", groupId = "my-group")
      public void consume(String message) throws IOException
      {
            if (!initiated) {
                  initiated = true;
                  String[] msgArgs = message.split(",");
                  int msgNumber = Integer.valueOf(msgArgs[0]);
                  countDown = msgNumber;
            } if (countDown <= 1) {
                  String[] msgArgs = message.split(",");
                  long startTime = Long.valueOf(msgArgs[1]);
                  long endTime = System.nanoTime();
                  long deltaTime = endTime-startTime;
                  long deltaTimeMilis = deltaTime/1_000_000;
                  System.out.println(String.valueOf(deltaTimeMilis));
                  initiated = false;
                  countDown = 0;
            } else {
                  countDown--;
            }

      }
}
      
