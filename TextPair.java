public class TextPair implements Comparable<TextPair> {
    private String roomID;
    private String userID;
    public void setRoomID(String roomID) {
        this.roomID = roomID;
    }

    public String getRoomID() {
        return roomID;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public int compareTo(TextPair o) {
        int c=roomID.compareTo(o.getRoomID());
        if(c!=0)return c;
        return userID.compareTo(o.getUserID());
    }

    @Override
    public boolean equals(Object o) {
        TextPair textPair=(TextPair)o;
        if(textPair.getRoomID().equals(this.getRoomID())&&textPair.getUserID().equals(this.getUserID())){
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.getUserID().hashCode()*163+this.getRoomID().hashCode();
    }

    @Override
    public String toString() {
        return this.getRoomID()+"\t"+this.getUserID();
    }
}
