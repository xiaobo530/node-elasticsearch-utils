import { Client, ApiResponse, RequestParams } from "@elastic/elasticsearch";
import pMap from "p-map";

/********************************* */
export interface ElasticConfig {
  url: string;
  keyPrefix?: string;
}

export interface IBaseDoc extends Record<string, any> {
  id?: string;
  _index?: string;
  _id?: string;
  _version?: number;
  _seq_no?: number;
  _primary_term?: number;
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

function toSimpleResult(apiResponse: any): any {
  const statusCode = apiResponse.statusCode;
  const { _index, _id, _version, result } = apiResponse.body;
  return { statusCode, _index, _id, _version, result } as SimpleResponseResult;
}

// function toDoc<T extends IBaseDoc>(apiResponse: ApiResponse): T {
function toDoc(apiResponse: any): any {
  const statusCode = apiResponse.statusCode;
  const { _index, _id, _version, _seq_no, _primary_term, _source, found } =
    apiResponse.body;
  if (found) {
    return { _index, _id, _version, _seq_no, _primary_term, ..._source };
  } else {
    return { _index, _id };
  }
}

//******************************* */

export function createElasticWrapper(cfg: ElasticConfig) {
  const client = new Client({ node: cfg.url });

  async function getById(
    index: string,
    ids: string | Array<string>,
    options?: RequestParams.Get
  ) {
    if (!Array.isArray(ids)) {
      ids = [ids];
    }

    const response = await pMap(
      ids,
      async (id) => {
        try {
          return await client.get({ ...options, index, id });
        } catch (error: any) {
          // console.log(error);
          return { body: error.meta.body };
        }
      },
      { concurrency: 5, stopOnError: false }
    );
    console.log(response);

    const result = response.map((res) => toDoc(res));

    console.log(result);
  }

  async function indexMany<T extends IBaseDoc>(
    index: string,
    docs: T | Array<T>,
    options?: Partial<RequestParams.Index>
  ): Promise<Array<SimpleResponseResult>> {
    if (!Array.isArray(docs)) {
      docs = [docs];
    }

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
  ): Promise<Array<SimpleResponseResult>> {
    if (!Array.isArray(ids)) {
      ids = [ids];
    }

    const response = await pMap(
      ids,
      async (id) => {
        try {
          return await client.delete({ ...options, index: indexName, id: id });
        } catch (error:any) {
          return { body: error.meta.body };
        }
      },
      { concurrency: 5 }
    );

    const result = response.map((res) => toSimpleResult(res));
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
    getById,
    deleteById,
    deleteByQuery,
    close,
  };
}

export type ElasticsearchWrapper = ReturnType<typeof createElasticWrapper>;
