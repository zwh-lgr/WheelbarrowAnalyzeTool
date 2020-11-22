package com.wheelanalyser.bean;

/*---------------------------------------
import com.amane.bean.converter.*;

import java.time.LocalDateTime;
----------------------------------------*/
import com.opencsv.bean.CsvBindByPosition;
//import com.opencsv.bean.CsvCustomBindByPosition;

public class Barrage {
    @CsvBindByPosition(position = 2)
    private String id;

    @CsvBindByPosition(position = 1)
    private String time;

    @CsvBindByPosition(position = 3)
    private String comment;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	@Override
	public String toString() {
		return "Barrage [comment=" + comment + ", id=" + id + ", time=" + time + "]";
	}

    
}
