import { describe, test, expect } from "@jest/globals";
import {
  createElasticWrapper,
  ElasticsearchWrapper,
} from "./elasticsearch-wrapper";
import { Client, ApiResponse, RequestParams } from "@elastic/elasticsearch";

describe("Elasticsearch Wrapper", () => {
  let wrapper: ElasticsearchWrapper;
  beforeAll(async () => {
    wrapper = createElasticWrapper({
      url: "http://localhost:9200",
    });
  });

  afterAll(() => {
    wrapper.close();
  });

  test("index one new doc", async () => {
    try {
      const indexName = "game-of-thrones";
      const doc = {
        character: "Ned Stark",
        quote: "Winter is coming.",
      };

      const result = await wrapper.indexMany(indexName, doc);

      console.log(result);

      expect(result![0].statusCode).toBe(201);
      expect(result![0].result).toBe("created");
    } catch (error) {
      console.log(error);
      throw error;
    }
  });

  test("index one doc with id", async () => {
    try {
      const indexName = "game-of-thrones";
      const doc = {
        id: "1234",
        character: "Ned Stark",
        quote: "Winter is coming.",
      };

      const result = await wrapper.indexMany(indexName, doc, {
        op_type: "index",
      });

      console.log(result);

      expect([201, 200]).toContain(result![0].statusCode);
      expect(["created", "updated"]).toContain(result![0].result);
    } catch (error) {
      console.log(error);
      throw error;
    }
  });

  test.only("index many docs", async () => {
    try {
      const indexName = "game-of-thrones";
      const docs = [
        {
          character: "Ned Stark",
          quote: "Winter is coming.",
        },
        {
          character: "Daenerys Targaryen",
          quote: "I am the blood of the dragon.",
        },
        {
          character: "Tyrion Lannister",
          quote: "A mind needs books like a sword needs a whetstone.",
        },
      ];
 
      const result = await wrapper.indexMany(indexName, docs); 

      console.log(result);

      expect(result.length).toBe(3);
      expect(result[0].statusCode).toBe(201);
      expect(result[0].result).toBe("created");
    } catch (error) {
      throw error;
    }
  });

  // test("delete a doc by id", async () => {
  //   try {
  //     const indexName = "game-of-thrones";
  //     const doc = {
  //       character: "Ned Stark",
  //       quote: "Winter is coming.",
  //     };

  //     const inserted = await wrapper.indexMany(indexName, doc);

  //     const id = inserted[0]._id;

  //     const result = await wrapper.deleteById(indexName, id!);

  //     console.log(result);
  //     expect(result[0].statusCode).toBe(200);
  //     expect(result[0].result).toBe("deleted");
  //   } catch (error) {
  //     throw error;
  //   }
  // });

  test("delete many docs by id", async () => {
    const wrapper = await createElasticWrapper({
      url: "http://localhost:9200",
    });
    expect(wrapper.client).not.toBeNull();

    const indexName = "game-of-thrones";
    const ids = ["nevZfoYBJS5NHzNLPS7G", "nOvZfoYBJS5NHzNLPS7F"];

    await wrapper.deleteById(indexName, ids);

    await wrapper.close();
  });

  test("delete many docs by query", async () => {
    const wrapper = await createElasticWrapper({
      url: "http://localhost:9200",
    });
    expect(wrapper.client).not.toBeNull();

    const indexName = "game-of-thrones";
    const search = {
      query: {
        match: {
          quote: "dragon",
        },
      },
    };

    const result = await wrapper.deleteByQuery(indexName, search);
    console.log(result);

    await wrapper.close();
  });

  test("get a doc", async () => {
    const wrapper = await createElasticWrapper({
      url: "http://localhost:9200",
    });
    expect(wrapper.client).not.toBeNull();

    const indexName = "game-of-thrones";
    const id = "pesnf4YBJS5NHzNLdy4K";

    const { body } = await wrapper.client.get({ index: indexName, id: id });
    console.log(body);

    await wrapper.close();
  });

  test("get many docs", async () => {
    const wrapper = await createElasticWrapper({
      url: "http://localhost:9200",
    });
    expect(wrapper.client).not.toBeNull();

    const indexName = "game-of-thrones";
    const ids = ["pesnf4YBJS5NHzNLdy4K", "p-s_f4YBJS5NHzNLCC6I"];

    const result = await Promise.all(
      ids.map((id) => wrapper.client.get({ index: indexName, id: id }))
    );
    console.log(result);

    const docs = result.map((res) => res.body._source);
    console.log(docs);

    await wrapper.close();
  });

  test("update one doc", async () => {
    const wrapper = await createElasticWrapper({
      url: "http://localhost:9200",
    });
    expect(wrapper.client).not.toBeNull();

    const indexName = "game-of-thrones";
    const doc = {
      character: "Ned Stark",
      quote: "Winter is coming.",
      isAlive: true,
    };

    await wrapper.indexMany(indexName, doc, { id: "1" });

    await wrapper.client.update({
      index: "game-of-thrones",
      id: "1",
      body: {
        doc: {
          isAlive: false,
        },
      },
    });

    const { body } = await wrapper.client.get({ index: indexName, id: "1" });
    console.log(body);

    await wrapper.close();
  });

  // test("get/set a string by keyPrefix: app", async () => {
  //   const client = await createRedisClient({
  //     url: "redis://127.0.0.1:6379/0",
  //     keyPrefix: "app:",
  //   });
  //   expect(client.ioredis).not.toBeNull();

  //   const key = "test:string";
  //   const value = "1234567890";
  //   await client.ioredis.set(key, value);
  //   const getvalue = await client.ioredis.get(key);
  //   expect(getvalue).toEqual(value);

  //   await client.close();
  // });
});
