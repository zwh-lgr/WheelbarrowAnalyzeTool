package com.wheelanalyser.bean;

import com.opencsv.bean.CsvBindByPosition;

public class Barrage {
	//以映射策略读取csv格式文件内容
    @CsvBindByPosition(position = 2)
    private String id;		//用户ID

    @CsvBindByPosition(position = 1)
    private String time;	//发言时间

    @CsvBindByPosition(position = 3)
    private String comment;	//发言内容，实际上本实现并未使用该字段

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
