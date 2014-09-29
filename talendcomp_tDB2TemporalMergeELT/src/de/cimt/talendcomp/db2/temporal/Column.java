package de.cimt.talendcomp.db2.temporal;

public class Column {

	private String name;
	private String dbType;
	private int length = 0;
	private int precision = 0;
	private String javaClass;
	private boolean notInSource = false;
	private boolean partOfSourceKey = false;
	private boolean trackChanges = false;
	private boolean createIndex = false;
	private boolean notNullable = false;
	private boolean preparedParam = false;
	private boolean autoIncrement = false;
	private boolean forInsert = true;
	private boolean forUpdate = true;

	public String getName() {
		return name;
	}

	public Column setName(String name) {
		if (name == null || name.isEmpty()) {
			throw new IllegalArgumentException("name cannot be null or empty");
		}
		this.name = name;
		return this;
	}

	public boolean isNotInSource() {
		return notInSource;
	}

	public void setNotInSource(boolean notInSource) {
		this.notInSource = notInSource;
	}

	public String getDbType() {
		return dbType;
	}

	public Column setDbType(String dbType) {
		this.dbType = dbType;
		return this;
	}

	public int getLength() {
		return length;
	}

	public Column setLength(Integer length) {
		if (length != null) {
			this.length = length;
		}
		return this;
	}

	public int getPrecision() {
		return precision;
	}

	public Column setPrecision(Integer precision) {
		if (precision != null) {
			this.precision = precision;
		}
		return this;
	}

	public String getJavaClass() {
		return javaClass;
	}

	public Column setJavaClass(String javaClass) {
		this.javaClass = javaClass;
		return this;
	}

	public boolean isPartOfSourceKey() {
		return partOfSourceKey;
	}

	public Column setPartOfSourceKey(Boolean partOfSourceKey) {
		if (partOfSourceKey != null) {
			this.partOfSourceKey = partOfSourceKey;
		}
		return this;
	}

	public boolean isTrackChanges() {
		return trackChanges && partOfSourceKey == false;
	}

	public Column setTrackChanges(Boolean trackChanges) {
		if (trackChanges != null) {
			this.trackChanges = trackChanges;
		}
		return this;
	}

	public boolean isCreateIndex() {
		return createIndex;
	}

	public Column setCreateIndex(Boolean createIndex) {
		if (createIndex != null) {
			this.createIndex = createIndex;
		}
		return this;
	}

	public boolean isNotNullable() {
		return notNullable || partOfSourceKey || autoIncrement;
	}

	public Column setNotNullable(Boolean notNullable) {
		if (notNullable != null) {
			this.notNullable = notNullable;
		}
		return this;
	}

	public boolean isPreparedParam() {
		return preparedParam;
	}

	public Column setPreparedParam(Boolean preparedParam) {
		if (preparedParam != null) {
			this.preparedParam = preparedParam;
		}
		return this;
	}

	public boolean isAutoIncrement() {
		return autoIncrement;
	}

	public Column setAutoIncrement(Boolean autoIncrement) {
		if (autoIncrement != null) {
			this.autoIncrement = autoIncrement;
		}
		return this;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof Column) {
			return name.toLowerCase().equals(((Column) o).name.toLowerCase());
		} else if (o instanceof String) {
			return name.toLowerCase().equals(((String) o).toLowerCase());
		} else {
			return false;
		}
	}
	
	public int hashCode() {
		return name.hashCode();
	}

	public boolean isForInsert() {
		return forInsert;
	}

	public void setForInsert(boolean forInsert) {
		this.forInsert = forInsert;
	}

	public boolean isForUpdate() {
		return forUpdate;
	}

	public void setForUpdate(boolean forUpdate) {
		this.forUpdate = forUpdate;
	}

}
