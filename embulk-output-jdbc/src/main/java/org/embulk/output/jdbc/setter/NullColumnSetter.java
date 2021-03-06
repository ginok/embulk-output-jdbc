package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;

import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;
import org.msgpack.value.Value;

public class NullColumnSetter
        extends ColumnSetter
{
    public NullColumnSetter(BatchInsert batch, JdbcColumn column,
            DefaultValueSetter defaultValue)
    {
        super(batch, column, defaultValue);
    }

    @Override
    public void booleanValue(boolean v) throws IOException, SQLException
    {
        defaultValue.setNull();
    }

    @Override
    public void longValue(long v) throws IOException, SQLException
    {
        defaultValue.setNull();
    }

    @Override
    public void doubleValue(double v) throws IOException, SQLException
    {
        defaultValue.setNull();
    }

    @Override
    public void stringValue(String v) throws IOException, SQLException
    {
        defaultValue.setNull();
    }

    @Override
    public void timestampValue(Timestamp v) throws IOException, SQLException
    {
        defaultValue.setNull();
    }

    @Override
    public void nullValue() throws IOException, SQLException
    {
        defaultValue.setNull();
    }

    @Override
    public void jsonValue(Value v) throws IOException, SQLException
    {
        defaultValue.setNull();
    }
}
