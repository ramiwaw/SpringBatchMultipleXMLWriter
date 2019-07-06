package com.example.springbatchmultiplexmlwriter.model;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="comment")
public class Comment {

	private int id;
	private String content;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

}
