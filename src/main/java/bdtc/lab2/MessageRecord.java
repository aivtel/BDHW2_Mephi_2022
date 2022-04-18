package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
    * Класс записи сообщения с добавленной группой
 */

@Data
@AllArgsConstructor
public class MessageRecord {

    private String sender;

    private String receiver;

    private String timestamp;

    private String messageText;

    private String groupName;
}

