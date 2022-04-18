package bdtc.lab2;

import java.io.Serializable;

/**
    * Класс элемента словаря группа-пользователь
 */

public class DictEntity implements Serializable {
    private String userName;

    private String groupName;

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserName() {
        return userName;
    }

    public String getGroupName() {
        return groupName;
    }
}
