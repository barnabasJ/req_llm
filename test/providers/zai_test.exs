defmodule ReqLLM.Providers.ZaiTest do
  @moduledoc """
  Provider-level tests for Z.AI implementation.
  """

  use ReqLLM.ProviderCase, provider: ReqLLM.Providers.Zai

  alias ReqLLM.Context
  alias ReqLLM.Message.ContentPart
  alias ReqLLM.Providers.Zai
  alias ReqLLM.Providers.ZaiCoder
  alias ReqLLM.Providers.ZaiCodingPlan

  describe "request preparation" do
    test "zai_coder uses the coding endpoint instead of registry metadata" do
      {:ok, request} =
        ZaiCoder.prepare_request(:chat, "zai_coder:glm-4.5-flash", "Hello", api_key: "test")

      assert request.options[:base_url] == "https://api.z.ai/api/coding/paas/v4"
    end

    test "zai_coder preserves explicit base_url overrides" do
      {:ok, request} =
        ZaiCoder.prepare_request(:chat, "zai_coder:glm-4.5-flash", "Hello",
          api_key: "test",
          base_url: "https://proxy.example.com/v1"
        )

      assert request.options[:base_url] == "https://proxy.example.com/v1"
    end
  end

  describe "encode_body/1" do
    test "drops assistant thinking parts when encoding history" do
      {:ok, model} = ReqLLM.model("zai:glm-4.5")

      context =
        Context.new([
          Context.user("Hi"),
          Context.assistant([ContentPart.thinking("internal"), ContentPart.text("hello")]),
          Context.user("What did you say?")
        ])

      request = %Req.Request{options: [context: context, model: model.model, stream: false]}

      encoded_request = Zai.encode_body(request)
      decoded = ReqLLM.Test.Helpers.json_body(encoded_request)

      [_user_msg, assistant_msg, _followup_msg] = decoded["messages"]

      assert assistant_msg["role"] == "assistant"
      assert assistant_msg["content"] == "hello"
    end

    test "handles map content parts with string keys" do
      {:ok, model} = ReqLLM.model("zai:glm-4.5")

      context =
        Context.new([
          Context.assistant([
            %{"type" => "thinking", "thinking" => "internal"},
            %{"type" => "text", "text" => "hello"}
          ]),
          Context.user("repeat")
        ])

      request = %Req.Request{options: [context: context, model: model.model, stream: false]}

      encoded_request = Zai.encode_body(request)
      decoded = ReqLLM.Test.Helpers.json_body(encoded_request)

      [assistant_msg, user_msg] = decoded["messages"]

      assert assistant_msg["content"] == "hello"
      assert user_msg["content"] == "repeat"
    end

    test "serializes tool_call_id and name on outbound tool messages" do
      {:ok, model} = ReqLLM.model("zai:glm-4.5")

      context =
        Context.new([
          Context.user("What is the weather in Vienna?"),
          Context.assistant("",
            tool_calls: [{"get_weather", %{"city" => "Vienna"}, [id: "call_weather_1"]}]
          ),
          Context.tool_result("call_weather_1", "get_weather", "21C, sunny")
        ])

      request = %Req.Request{options: [context: context, model: model.model, stream: false]}

      encoded_request = Zai.encode_body(request)
      decoded = ReqLLM.Test.Helpers.json_body(encoded_request)

      [_user_msg, assistant_msg, tool_msg] = decoded["messages"]

      assert assistant_msg["tool_calls"] == [
               %{
                 "id" => "call_weather_1",
                 "type" => "function",
                 "function" => %{"name" => "get_weather", "arguments" => ~s({"city":"Vienna"})}
               }
             ]

      assert tool_msg["role"] == "tool"
      assert tool_msg["content"] == "21C, sunny"
      assert tool_msg["tool_call_id"] == "call_weather_1"
      assert tool_msg["name"] == "get_weather"
    end

    test "omits tool_call_id and name when the message carries none" do
      {:ok, model} = ReqLLM.model("zai:glm-4.5")

      context =
        Context.new([
          Context.user("Hi"),
          Context.assistant("hello")
        ])

      request = %Req.Request{options: [context: context, model: model.model, stream: false]}

      encoded_request = Zai.encode_body(request)
      decoded = ReqLLM.Test.Helpers.json_body(encoded_request)

      [user_msg, assistant_msg] = decoded["messages"]

      assert user_msg["content"] == "Hi"
      assert assistant_msg["content"] == "hello"
      refute Map.has_key?(user_msg, "tool_call_id")
      refute Map.has_key?(user_msg, "name")
      refute Map.has_key?(assistant_msg, "tool_call_id")
      refute Map.has_key?(assistant_msg, "name")
    end

    test "zai_coding_plan encode_body preserves tool-result correlation" do
      context =
        Context.new([
          Context.assistant("",
            tool_calls: [{"get_weather", %{"city" => "Vienna"}, [id: "call_cp_1"]}]
          ),
          Context.tool_result("call_cp_1", "get_weather", "21C, sunny")
        ])

      {:ok, request} =
        ZaiCodingPlan.prepare_request(:chat, "zai_coding_plan:glm-4.7", context, api_key: "test")

      decoded = ReqLLM.Test.Helpers.json_body(ZaiCodingPlan.encode_body(request))

      [_assistant_msg, tool_msg] = decoded["messages"]

      assert tool_msg["role"] == "tool"
      assert tool_msg["content"] == "21C, sunny"
      assert tool_msg["tool_call_id"] == "call_cp_1"
      assert tool_msg["name"] == "get_weather"
    end
  end
end
