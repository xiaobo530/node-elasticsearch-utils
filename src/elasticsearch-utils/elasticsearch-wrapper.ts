import { Client, ApiResponse, RequestParams } from "@elastic/elasticsearch";
import pMap from "p-map";

/********************************* */
export interface ElasticConfig {
  url: string;
  keyPrefix?: string;
}

export interface IBaseDoc extends Record<string, any> {
  id?: string;
  [index: string]: any;
}

export interface SimpleResponseResult {
  statusCode: number;
  _index?: string;
  _id?: string;
  _version?: number;
  result?: string;
}

export interface InsertResult {
  statusCode: number;
  _index?: string;
  _id?: string;
  _version?: number;
  result?: string;
}

//******************************* */

function toIndexRequest<T extends IBaseDoc>(
  index: string,
  doc: T,
  options?: Partial<RequestParams.Index>
): RequestParams.Index<T> {
  return {
    ...options,
    index: index,
    body: doc,
    id: doc.id ?? undefined,
  };
}

function toSimpleResult(apiResponse: ApiResponse): SimpleResponseResult {
  const statusCode = apiResponse.statusCode;
  const { _index, _id, _version, result } = apiResponse.body;
  return { statusCode, _index, _id, _version, result } as SimpleResponseResult;
}

//******************************* */

export function createElasticWrapper(cfg: ElasticConfig) {
  const client = new Client({ node: cfg.url });

  function get(
    indexName: string,
    ids: string | Array<string>,
    options?: RequestParams.Get
  ) {
    if (!Array.isArray(ids)) {
      ids = [ids];
    }
    // Promise.all(
    //   ids.map((id) => {
    //    const  client.get({ ...options, index: indexName, id: id });
    //   })
    // );
    // const indexName = "game-of-thrones";
    // const id = "pesnf4YBJS5NHzNLdy4K";

    // const { body } = await helper.client.get({ index: indexName, id: id });
    // console.log(body);
  }

  async function indexMany<T extends IBaseDoc>(
    index: string,
    docs: T | Array<T>,
    options?: Partial<RequestParams.Index>
  ): Promise<Array<SimpleResponseResult>> {
    if (!Array.isArray(docs)) {
      docs = [docs];
    }

    // const response = await Promise.all(
    //   docs.map((doc) => client.index(toIndexRequest(index, doc, options)))
    // );

    // const response = docs.map(async (doc) => {
    //   return await client.index(toIndexRequest(index, doc, options));
    // }).forEach(async (val)=>await val);

    const response = await pMap(
      docs,
      async (doc) => {
        return await client.index(toIndexRequest(index, doc, options));
      },
      { concurrency: 5 }
    );
    // console.log(response);

    const result = response.map((res) => toSimpleResult(res));
    // console.log(result);

    return result;
  }

  async function deleteById(
    indexName: string,
    ids: string | Array<string>,
    options?: RequestParams.Delete
  ): Promise<Array<InsertResult>> {
    if (!Array.isArray(ids)) {
      ids = [ids];
    }
    const response = await Promise.all(
      ids.map((id) => client.delete({ ...options, index: indexName, id: id }))
    );

    console.log(response);

    const result: Array<InsertResult> = response.map((res) => {
      const statusCode = res.statusCode;
      const { _index, _id, _version, result } = res.body;
      return { statusCode, _index, _id, _version, result } as InsertResult;
    });

    return result;
  }

  async function deleteByQuery(
    indexNames: string | Array<string>,
    query: Record<string, any>
  ): Promise<boolean> {
    const { body, statusCode } = await client.deleteByQuery({
      index: indexNames,
      body: query,
    });

    return statusCode == 200;
  }

  async function close() {
    await client.close();
  }
  return {
    client,
    indexMany,
    deleteById,
    deleteByQuery,
    close,
  };
}

export type ElasticsearchWrapper = ReturnType<typeof createElasticWrapper>;
