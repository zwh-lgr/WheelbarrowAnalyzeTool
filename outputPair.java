public class outputPair implements Comparable<outputPair> {
    private String roomID;
    private String userID;
    private Integer sum;
    private Double coeffient;

    public String getRoomID() {
        return roomID;
    }

    public String getUserID() {
        return userID;
    }

    public Integer getSum() {
        return sum;
    }

    public Double getCoeffient() {
        return coeffient;
    }

    public void setRoomID(String roomID) {
        this.roomID = roomID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public void setSum(Integer sum) {
        this.sum = sum;
    }

    public void setCoeffient(Double coeffient) {
        this.coeffient = coeffient;
    }

    @Override
    public int compareTo(outputPair o) {
        int c=this.getSum().compareTo(o.getSum());
        if(c!=0)return c;
        return this.getCoeffient().compareTo(o.getCoeffient());
    }

    @Override
    public String toString() {
        return this.getRoomID()+"\t"+this.getUserID()+"\t"+this.getSum().toString()+"\t"+this.getCoeffient();
    }
}
