defmodule ProtocolTest do
  use ExUnit.Case

  test "to_simple_error with empty string" do
    assert_raise ArgumentError, fn -> Redis.Protocol.to_simple_error("") end
  end

  test "to_simple_error with non-empty string" do
    assert Redis.Protocol.to_simple_error("foo") == "-foo\r\n"
  end

  test "to_simple_string with empty string" do
    assert_raise ArgumentError, fn -> Redis.Protocol.to_simple_string("") end
  end

  test "to_simple_string with non-empty string" do
    assert Redis.Protocol.to_simple_string("foo") == "+foo\r\n"
  end

  test "to_bulk_string with empty string" do
    assert Redis.Protocol.to_bulk_string("") == "$0\r\n\r\n"
  end

  test "to_bulk_string with non-empty string" do
    assert Redis.Protocol.to_bulk_string("foo") == "$3\r\nfoo\r\n"
  end

  test "to_bulk_string_array with empty list" do
    assert Redis.Protocol.to_bulk_string_array([]) == "*0\r\n"
  end

  test "to_bulk_string_array with single value" do
    assert Redis.Protocol.to_bulk_string_array(["foo"]) == "*1\r\n$3\r\nfoo\r\n"
  end

  test "to_bulk_string_array with multiple values" do
    assert Redis.Protocol.to_bulk_string_array(["foo", "lorem"]) ==
             "*2\r\n$3\r\nfoo\r\n$5\r\nlorem\r\n"
  end

  test "validate_stream_id valid id without previous entry" do
    assert Redis.Protocol.validate_stream_id("0-1") == :ok
    assert Redis.Protocol.validate_stream_id("1-1") == :ok
    assert Redis.Protocol.validate_stream_id("1-0") == :ok
  end

  test "validate_stream_id invalid id without previous entry" do
    assert Redis.Protocol.validate_stream_id("0-0") == :invalid
  end

  test "validate_stream_id valid id with older previous entry" do
    assert Redis.Protocol.validate_stream_id("1-2", "1-1") == :ok
    assert Redis.Protocol.validate_stream_id("2-1", "1-1") == :ok
    assert Redis.Protocol.validate_stream_id("2-2", "1-2") == :ok
  end

  test "validate_stream_id valid id with newer previous entry" do
    assert Redis.Protocol.validate_stream_id("1-1", "1-1") == :too_small
    assert Redis.Protocol.validate_stream_id("1-2", "1-2") == :too_small
    assert Redis.Protocol.validate_stream_id("2-1", "2-1") == :too_small
    assert Redis.Protocol.validate_stream_id("1-1", "2-2") == :too_small
  end
end
