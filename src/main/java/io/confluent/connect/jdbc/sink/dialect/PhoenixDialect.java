/*
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.jdbc.sink.dialect;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;

import java.util.Collection;
import java.util.List;

import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.joinToBuilder;
import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.nCopiesToBuilder;

public class PhoenixDialect extends DbDialect {

  public PhoenixDialect() {
    super("", "");
  }

  @Override
  public String getCreateQuery(String tableName, Collection<SinkRecordField> fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getAlterTable(String tableName, Collection<SinkRecordField> fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getInsert(final String tableName, final Collection<String> keyColumns, final Collection<String> nonKeyColumns) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getUpsertQuery(final String table, Collection<String> keyCols, Collection<String> cols) {
    StringBuilder builder = new StringBuilder("UPSERT INTO ");
    builder.append(escaped(table));
    builder.append("(");
    joinToBuilder(builder, ",", keyCols, cols, escaper());
    builder.append(") VALUES(");
    nCopiesToBuilder(builder, ",", "?", keyCols.size() + cols.size());
    builder.append(")");
    return builder.toString();
  }
}
