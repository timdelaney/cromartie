package com.tim.cromartie;

public class Column {
		String name;
		String type;
		int size;
		
		@Override
		public String toString() {
			return "Column [name=" + name + ", type=" + type + ", size=" + size + "]";
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getType() {
			return type;
		}
		public void setType(String type) {
			this.type = type;
		}
		public int getSize() {
			return size;
		}
		public void setSize(int size) {
			this.size = size;
		}
}
