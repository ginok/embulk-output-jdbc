package org.embulk.output.jdbc;

import java.util.List;
import java.util.Map;

import org.embulk.config.ConfigException;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

import com.google.common.base.Optional;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableMap;

import org.embulk.spi.Exec;

public class JdbcSchema
{
    private List<JdbcColumn> columns;
    private Map<String, JdbcColumnOption> column_options;

    @JsonCreator
    public JdbcSchema(List<JdbcColumn> columns)
    {
        this.columns = columns;
        ImmutableMap.Builder<String, JdbcColumnOption> builder = ImmutableMap.builder();
        this.column_options = builder.build();
    }

    public JdbcSchema(List<JdbcColumn> columns, Map<String, JdbcColumnOption> column_options)
    {
        this.columns = columns;
        this.column_options = column_options;
    }

    @JsonValue
    public List<JdbcColumn> getColumns()
    {
        return columns;
    }

    public Map<String, JdbcColumnOption> getColumnOptions()
    {
        return column_options;
    }

    public Optional<JdbcColumnOption> findColumnOption(String name)
    {
        return Optional.fromNullable(column_options.get(name));
    }

    public Optional<JdbcColumn> findColumn(String name)
    {
        // because both upper case column and lower case column may exist, search twice
        for (JdbcColumn column : columns) {
            if (column.getName().equals(name)) {
                return Optional.of(column);
            }
        }

        JdbcColumn foundColumn = null;
        for (JdbcColumn column : columns) {
            if (column.getName().equalsIgnoreCase(name)) {
                if (foundColumn != null) {
                    throw new ConfigException(String.format("Cannot specify column '%s' because both '%s' and '%s' exist.",
                            name, foundColumn.getName(), column.getName()));
                }
                foundColumn = column;
            }
        }

        if (foundColumn != null) {
            return Optional.of(foundColumn);
        }
        return Optional.absent();
    }

    public int getCount()
    {
        return columns.size();
    }

    public JdbcColumn getColumn(int i)
    {
        return columns.get(i);
    }

    public String getColumnName(int i)
    {
        return columns.get(i).getName();
    }

    public static JdbcSchema filterSkipColumns(JdbcSchema schema, Map<String, JdbcColumnOption> columnOptions)
    {
        ImmutableList.Builder<JdbcColumn> builder = ImmutableList.builder();
        ImmutableMap.Builder<String, JdbcColumnOption> builder_for_options = ImmutableMap.builder();
        for (JdbcColumn c : schema.getColumns()) {
            if (!c.isSkipColumn()) {
                builder.add(c);
                builder_for_options.put(c.getName(), columnOptionOf(columnOptions, c.getName()));
            }
        }
        return new JdbcSchema(builder.build(), builder_for_options.build());
    }

    private static JdbcColumnOption columnOptionOf(Map<String, JdbcColumnOption> columnOptions, String columnName)
    {
        return Optional.fromNullable(columnOptions.get(columnName)).or(
                    // default column option
                    new Supplier<JdbcColumnOption>()
                    {
                        public JdbcColumnOption get()
                        {
                            return Exec.newConfigSource().loadConfig(JdbcColumnOption.class);
                        }
                    });
    }
}
