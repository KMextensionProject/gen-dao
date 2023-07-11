package com.gratex.gendao.db;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Convertible map which provides convenient methods for getting value in
 * desired type if possible
 */
public class TypeMap extends LinkedHashMap<String, Object> {

	private static final long serialVersionUID = -2690058775910305972L;

	public TypeMap() {
		super();
	}

	public TypeMap(String stringKey, Object value, Object... keyValuePairs) {
		put(stringKey, value);

		int length = keyValuePairs.length;
		if (length == 0) {
			return;
		} else if (length % 2 != 0) {
			throw new IllegalArgumentException("Map must consists of even number of arguments/pairs (key;value)");
		}

		for (int i = 0, j = 1; j < length; i += 2, j += 2) {
			String firstObject = String.valueOf(keyValuePairs[i]);
			Object secondObject = keyValuePairs[j];
			put(firstObject, secondObject);
		}
	}

	public TypeMap(Map<String, Object> map) {
		putAll(map);
	}

	public Integer getInteger(String key) {
		Object object = this.get(key);
		if (object == null) {
			return null;
		} else if (object instanceof Integer) {
			return (Integer) object;
		} else if (object instanceof CharSequence) {
			String stringValue = (String) object;
			if (stringValue.isEmpty()) {
				return null;
			}
			try {
				return Integer.parseInt(stringValue);
			} catch (NumberFormatException nfe) {
				// do nothing
			}
		}
		throw new InconvertableTypeException(object, Integer.class);
	}

	public Long getLong(String key) {
		Object object = this.get(key);
		if (object == null) {
			return null;
		} else if (object instanceof Long) {
			return (Long) object;
		} else if (object instanceof Integer) {
			return ((Integer) object).longValue();
		}
		throw new InconvertableTypeException(object, Long.class);
	}

	public String getString(String key) {
		Object object = this.get(key);
		if (object == null) {
			return null;
		} else {
			return String.valueOf(object);
		}
	}

	public UUID getUUID(String key) {
		Object object = this.get(key);
		if (object == null) {
			return null;
		} else if (object instanceof UUID) {
			return (UUID) object;
		} else if (object instanceof String) {
			String uuid = (String) object;
			if (uuid.trim().isEmpty()) {
				return null;
			}
			try {
				return UUID.fromString(uuid);
			} catch (IllegalArgumentException e) {
				// do nothing
			}
		}
		throw new InconvertableTypeException(object, UUID.class);
	}

	public Boolean getBoolean(String key) {
		Object object = this.get(key);
		if (object == null) {
			return null;
		} else if (object instanceof Boolean) {
			return (Boolean) object;
		} else if (object instanceof String) {
			try {
				return Boolean.parseBoolean(key);
			} catch (IllegalArgumentException e) {
				// proper exception will be thrown at the bottom
			}
		} else if (object instanceof Integer) {
			if ((Integer) object == 1) {
				return true;
			} else if ((Integer) object == 0) {
				return false;
			}
		}
		throw new InconvertableTypeException(object, Boolean.class);
	}

	public boolean getBooleanDefault(String key, boolean defaultValue) {
		Boolean result = getBoolean(key);
		return result == null ? defaultValue : result;
	}

	public LocalDate getLocalDate(String key) {
		Object object = this.get(key);
		if (object == null) {
			return null;
		} else if (object instanceof LocalDate) {
			return (LocalDate) object;
		} else if (object instanceof java.sql.Date) {
			java.sql.Date date = (java.sql.Date) object;
			return date.toLocalDate();
		}

		throw new InconvertableTypeException(object, LocalDate.class);
	}

	public LocalDateTime getLocalDateTime(String key) {
		Object object = this.get(key);
		if (object == null) {
			return null;
		} else if (object instanceof LocalDateTime) {
			return (LocalDateTime) object;
		} else if (object instanceof java.sql.Timestamp) {
			java.sql.Timestamp date = (java.sql.Timestamp) object;
			return date.toLocalDateTime();
		}

		throw new InconvertableTypeException(object, LocalDateTime.class);
	}

	public Object[] toArray() {
		return this.values().toArray();
	}
}
