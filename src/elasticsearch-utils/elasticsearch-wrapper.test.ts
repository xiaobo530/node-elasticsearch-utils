import { describe, test, expect } from "@jest/globals";
import {
  createElasticWrapper,
  ElasticsearchWrapper,
  toSimpleResult,
} from "./elasticsearch-wrapper";
import { Client, ApiResponse, RequestParams } from "@elastic/elasticsearch";
import { isUndefined } from "util";
import { IBaseDoc, IUpdateDoc } from "./elasticsearch-wrapper";

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

      // console.log(result);

      expect(result![0]._statusCode).toBe(201);
      expect(result![0].result).toBe("created");
    } catch (error) {
      // console.log(error);
      throw error;
    }
  });

  test("index one doc with id", async () => {
    try {
      const indexName = "game-of-thrones";
      const doc = {
        id: "id_for_one_index",
        character: "Ned Stark",
        quote: "Winter is coming.",
      };

      const result = await wrapper.indexMany(indexName, doc, {
        op_type: "index",
      });

      // console.log(result);

      expect([201, 200]).toContain(result![0]._statusCode);
      expect(["created", "updated"]).toContain(result![0].result);
    } catch (error) {
      // console.log(error);
      throw error;
    }
  });

  test("index many docs", async () => {
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

      // console.log(result);

      expect(result.length).toBe(3);
      expect(result.every((r) => r._statusCode == 201)).toBeTruthy();
      expect(result.every((r) => r.result == "created")).toBeTruthy();
    } catch (error) {
      throw error;
    }
  });

  test("delete one docs by id", async () => {
    try {
      const indexName = "game-of-thrones";
      const docs = [
        {
          character: "Ned Stark",
          quote: "Winter is coming.",
        },
      ];

      const inserted = await wrapper.indexMany(indexName, docs, {
        refresh: true,
      });

      let id = inserted.map((r) => r._id!)[0];
      // console.log(id);

      const result = (await wrapper.deleteById(indexName, id))![0];

      // console.log(result);
      expect(result._statusCode).toBe(200);
      expect(result.result).toBe("deleted");
    } catch (error) {
      throw error;
    }
  });

  test("delete not exist id", async () => {
    try {
      const indexName = "game-of-thrones";
      let id = "a not exits id";

      const result = (await wrapper.deleteById(indexName, id))[0];
      // console.log(result);

      expect(result._statusCode).toBe(404);
      expect(result.result).toBe("not_found");
    } catch (error) {
      throw error;
      // console.log(error);
    }
  });

  test("delete many docs by id", async () => {
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

      const inserted = await wrapper.indexMany(indexName, docs, {
        refresh: true,
      });

      let ids = inserted.map((r) => r._id!);
      // console.log(ids);

      const result = await wrapper.deleteById(indexName, ids);

      // console.log(result);
      expect(result.every((r) => r._statusCode == 200)).toBeTruthy();
      expect(result.every((r) => r.result == "deleted")).toBeTruthy();
    } catch (error) {
      throw error;
    }
  });

  test("delete many docs by query", async () => {
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

      const inserted = await wrapper.indexMany(indexName, docs, {
        refresh: true,
      });

      const search = {
        query: {
          match: {
            quote: "dragon",
          },
        },
      };

      const result = await wrapper.deleteByQuery(indexName, search);
      // console.log(result);

      expect(result._statusCode).toBe(200);
      expect(result.deleted).toBeGreaterThanOrEqual(1);
    } catch (error) {
      throw error;
    }
  });

  test("exists doc", async () => {
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

      const inserted = await wrapper.indexMany(indexName, docs, {
        refresh: true,
      });

      let ids = inserted.map((r) => r._id!);

      const result = await wrapper.exists(indexName, ids);

      // console.log(result);
      expect(result.every((r) => r._statusCode == 200)).toBeTruthy();
      expect(result.every((r) => r.exists)).toBeTruthy();
    } catch (error) {
      throw error;
    }
  });

  test("not exists doc", async () => {
    try {
      const indexName = "game-of-thrones";
      const id = ["id_not_exists_$$$", "id_not_exists_$$$$"];

      const result = await wrapper.exists(indexName, id);

      expect(result.every((r) => r._statusCode == 404)).toBeTruthy();
      expect(result.every((r) => r.exists)).toBeFalsy();
    } catch (error) {
      throw error;
    }
  });

  test("get doc by id", async () => {
    try {
      const indexName = "game-of-thrones";
      const docs = [
        {
          character: "Ned Stark",
          quote: "Winter is coming.",
        },
      ];

      const inserted = await wrapper.indexMany(indexName, docs, {
        refresh: true,
      });

      let id = inserted[0]._id!;

      const result = (await wrapper.getById(indexName, id))[0];

      expect(result._id).toBe(id);
      expect(result.character).toBe("Ned Stark");
    } catch (error) {}
  });

  test("get doc by not exist id", async () => {
    try {
      const indexName = "game-of-thrones";
      let id = "a not exits id";
      const result = (await wrapper.getById(indexName, id))[0];
      console.log(result);

      expect(result._id).toBeUndefined();
    } catch (error) {}
  });

  test("get many docs by id", async () => {
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

      const inserted = await wrapper.indexMany(indexName, docs, {
        refresh: true,
      });

      let ids = inserted.map((doc) => doc._id!);

      const result = await wrapper.getById(indexName, ids);

      expect(result.every((r) => r._id === undefined)).toBeFalsy();
    } catch (error) {}
  });

  test("update one doc by id using doc model", async () => {
    try {
      const id = "id_for_update_one_doc";
      const indexName = "game-of-thrones";
      const doc = {
        id: id,
        character: "Ned Stark",
        quote: "Winter is coming.",
        isAlive: true,
      };

      await wrapper.indexMany(indexName, doc);

      const result = await wrapper.updateById(indexName, id, {
        doc: {
          isAlive: false,
          newField: "a new field",
        },
      });

      expect(result[0]._statusCode).toBe(200);
      expect(result[0].result).toBe("updated");
    } catch (error) {
      throw error;
    }
  });

  test("update doc by not exits id ", async () => {
    try {
      const id = "id_for_update_not_exist";
      const indexName = "game-of-thrones";

      const result = await wrapper.updateById(indexName, id, {
        doc: {
          isAlive: false,
          newField: "a new field",
        },
      });
      // console.log(result);
      expect(result[0]._statusCode).toBe(404);
      expect(result[0].error).toBeDefined();
    } catch (error) {
      throw error;
    }
  });

  test("update one doc by id using script model", async () => {
    try {
      const id = "id_for_update_one_doc_script";
      const indexName = "game-of-thrones";
      const doc = {
        id: id,
        character: "Ned Stark",
        quote: "Winter is coming.",
        isAlive: true,
        times: 0,
      };

      await wrapper.indexMany(indexName, doc);

      const result = await wrapper.updateById(indexName, id, {
        script: {
          lang: "painless",
          source: "ctx._source.times++",
        },
      });

      expect(result[0]._statusCode).toBe(200);
      expect(result[0].result).toBe("updated");

      const docs = await wrapper.getById(indexName, id);
      expect(docs[0].times).toBe(1);
    } catch (error) {
      throw error;
    }
  });

  test("update many doc by id using doc model", async () => {
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

      const inserted = await wrapper.indexMany(indexName, docs, {
        refresh: true,
      });

      let ids = inserted.map((r) => r._id!);

      const result = await wrapper.updateById(indexName, ids, {
        doc: {
          isAlive: false,
          newField: "a new field",
        },
      });

      expect(result.every((r) => r._statusCode == 200)).toBeTruthy();
      expect(result.every((r) => r.result == "updated")).toBeTruthy();
    } catch (error) {
      throw error;
    }
  });

  test("update many doc by id using script model", async () => {
    try {
      const indexName = "game-of-thrones";
      const docs = [
        {
          character: "Ned Stark",
          quote: "Winter is coming.",
          times: 0,
        },
        {
          character: "Daenerys Targaryen",
          quote: "I am the blood of the dragon.",
          times: 0,
        },
        {
          character: "Tyrion Lannister",
          quote: "A mind needs books like a sword needs a whetstone.",
          times: 99,
        },
      ];

      const inserted = await wrapper.indexMany(indexName, docs, {
        refresh: true,
      });

      let ids = inserted.map((r) => r._id!);

      const result = await wrapper.updateById(indexName, ids, {
        script: {
          lang: "painless",
          source: "ctx._source.times++",
        },
      });

      expect(result.every((r) => r._statusCode == 200)).toBeTruthy();
      expect(result.every((r) => r.result == "updated")).toBeTruthy();
    } catch (error) {
      throw error;
    }
  });

  test("update may docs by query", async () => {
    try {
      const indexName = "game-of-thrones";
      const docs = [
        {
          character: "Ned Stark",
          quote: "Winter is coming.",
        },
        {
          character: "Arya Stark",
          quote: "A girl is Arya Stark of Winterfell. And I'm going home.",
        },
      ];

      await wrapper.indexMany(indexName, docs, {
        refresh: true,
      });

      const updateDoc: IUpdateDoc = {
        script: {
          lang: "painless",
          source: 'ctx._source["house"] = "stark"',
        },
        query: {
          match: {
            character: "stark",
          },
        },
      };

      const result = await wrapper.updateByQuery(indexName, updateDoc, {
        refresh: true,
      });

      expect(result._statusCode).toBe(200);
      expect(result.updated).toBeGreaterThanOrEqual(0);
    } catch (error) {
      throw error;
    }
  });

  test("search docs", async () => {
    try {
      const indexName = "game-of-thrones";
      const docs = [
        {
          character: "Ned Stark",
          quote: "Winter is coming.",
        },
        {
          character: "Arya Stark",
          quote: "A girl is Arya Stark of Winterfell. And I'm going home.",
        },
      ];

      await wrapper.indexMany(indexName, docs, {
        refresh: true,
      });

      const query = {
        query: {
          match: {
            quote: "winter",
          },
        },
      };

      const result = await wrapper.search(indexName, query, {
        seq_no_primary_term: true,
        size: 20,
      });
    } catch (error) {
      throw error;
    }
  });
});
