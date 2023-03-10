import { Client, ApiResponse, RequestParams } from "@elastic/elasticsearch";
import pMap from "p-map";
import { expect } from "@jest/globals";

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
  _found?: boolean;
  [index: string]: any;
}

export interface IUpdateDoc {
  doc?: Record<string, any>;
  script?: {
    lang: "painless";
    source: string;
    params?: Record<string, any>;
  };
  query?: Record<string, any>;
  [index: string]: any;
}

export interface ActionResult {
  _statusCode: 200 | 201 | 404 | null;
  _index?: string;
  _id?: string;
  _version?: number;
  _seq_no?: number;
  _primary_term?: number;
  result?: "created" | "deleted" | "updated";
  total?: number;
  updated?: number;
  deleted?: number;
  error?: any;
  exist?: boolean;
  [index: string]: any;
}

export function toDoc<T extends IBaseDoc = any>(apiResponse: any): T {
  // function toDoc(apiResponse: any): any {
  const statusCode = apiResponse.statusCode;
  const { _index, _id, _version, _seq_no, _primary_term, _source, found } =
    apiResponse.body;
  if (found) {
    return { _index, _id, _version, _seq_no, _primary_term, ..._source };
  } else {
    return { _index, _id: undefined } as T;
  }
}

//******************************* */

export function createElasticWrapper(cfg: ElasticConfig) {
  const client = new Client({ node: cfg.url });

  /**
   * check doc exists by id
   * @param index
   * @param ids
   * @param options
   * @returns
   */
  async function exists(
    index: string,
    ids: string | Array<string>,
    options?: RequestParams.Exists
  ): Promise<Array<ActionResult>> {
    if (!Array.isArray(ids)) {
      ids = [ids];
    }

    const response = await pMap(
      ids,
      async (id) => {
        try {
          const { body, statusCode } = await client.exists({
            ...options,
            index,
            id,
          });
          return {
            exists: body,
            _statusCode: statusCode,
            _id: id,
            _index: index,
          } as ActionResult;
        } catch (error: any) {
          return {
            exists: false,
            _statusCode: 404,
            _id: id,
            _index: index,
            error: error,
          } as ActionResult;
        }
      },
      { concurrency: 5, stopOnError: false }
    );

    return response;
  }

  /**
   * get docs by id
   * @param index
   * @param ids
   * @param options
   * @returns
   */
  async function getById<T extends IBaseDoc = IBaseDoc>(
    index: string,
    ids: string | Array<string>,
    options?: RequestParams.Get
  ): Promise<Array<T>> {
    if (!Array.isArray(ids)) {
      ids = [ids];
    }

    const response = await pMap(
      ids,
      async (id) => {
        try {
          const { statusCode, body } = await client.get({
            ...options,
            index,
            id,
          });
          const {
            _index,
            _id,
            _version,
            _seq_no,
            _primary_term,
            _source,
            found,
          } = body;
          return {
            _statusCode: statusCode,
            _index,
            _id,
            _seq_no,
            _primary_term,
            _found: found,
            _version,
            ..._source,
          };
        } catch (error: any) {
          const { statusCode, body } = error.meta;
          const { _index, _id, found } = body;
          return { _statusCode: statusCode, _index, _id, _found: found };
        }
      },
      { concurrency: 5, stopOnError: false }
    );
    // console.log(response);

    return response;
  }

  /**
   * index docs
   * @param index
   * @param docs
   * @param options
   * @returns
   */
  async function indexMany<T extends IBaseDoc>(
    index: string,
    docs: T | Array<T>,
    options?: Partial<RequestParams.Index>
  ): Promise<Array<ActionResult>> {
    if (!Array.isArray(docs)) {
      docs = [docs];
    }

    const response = await pMap(
      docs,
      async (doc) => {
        const { statusCode, body } = await client.index({
          ...options,
          index,
          body: doc,
          id: doc.id,
        });
        const { _index, _id, _seq_no, _primary_term, result } = body;
        return {
          _statusCode: statusCode,
          _index,
          _id,
          _seq_no,
          _primary_term,
          result,
        } as ActionResult;
      },
      { concurrency: 5 }
    );

    return response;
  }

  /**
   * delete docs by id(s)
   * @param indexName
   * @param ids
   * @param options
   * @returns
   */
  async function deleteById(
    indexName: string,
    ids: string | Array<string>,
    options?: Partial<RequestParams.Delete>
  ): Promise<Array<ActionResult>> {
    if (!Array.isArray(ids)) {
      ids = [ids];
    }

    const response = await pMap(
      ids,
      async (id) => {
        try {
          const { statusCode, body } = await client.delete({
            ...options,
            index: indexName,
            id: id,
          });
          const { _index, _id, _seq_no, _primary_term, result } = body;
          return {
            _statusCode: statusCode,
            _index,
            _id,
            _seq_no,
            _primary_term,
            result,
          } as ActionResult;
        } catch (error: any) {
          const { statusCode, body } = error.meta;
          const { _index, _id, _seq_no, _primary_term, result } = body;
          return {
            _statusCode: statusCode,
            _index,
            _id,
            _seq_no,
            _primary_term,
            result,
            error: error,
          };
        }
      },
      { concurrency: 5 }
    );

    return response;
  }

  /**
   * delete docs by query
   * @param indexNames
   * @param query
   * @param options
   * @returns
   */
  async function deleteByQuery<T = Record<string, any>>(
    indexNames: string | Array<string>,
    query: Record<string, any>,
    options?: Partial<RequestParams.DeleteByQuery>
  ): Promise<ActionResult> {
    const response = await client.deleteByQuery({
      ...options,
      index: indexNames,
      body: query,
    });
    const { statusCode, body } = response;
    const { deleted, total } = body;

    const result = { _statusCode: statusCode, deleted, total } as ActionResult;

    return result;
  }

  /**
   * update one or many docs by id
   * @param indexName
   * @param ids
   * @param updateDoc
   * @param options
   * @returns
   */
  async function updateById<T extends IUpdateDoc>(
    indexName: string,
    ids: string | Array<string>,
    updateDoc: T,
    options?: Partial<RequestParams.Update>
  ): Promise<Array<ActionResult>> {
    try {
      if (!Array.isArray(ids)) {
        ids = [ids];
      }

      const response = await pMap(
        ids,
        async (id) => {
          try {
            const { statusCode, body } = await client.update({
              ...options,
              index: indexName,
              id: id,
              body: updateDoc,
            });
            const { _index, _id, _seq_no, _primary_term, result } = body;
            return {
              _statusCode: statusCode,
              _index,
              _id,
              _seq_no,
              _primary_term,
              result,
            } as ActionResult;
          } catch (error: any) {
            const { statusCode, body } = error.meta;
            const { _index, _id, _seq_no, _primary_term, result } = body;
            return {
              _statusCode: statusCode,
              _index,
              _id,
              _seq_no,
              _primary_term,
              result,
              error: error,
            };
          }
        },
        { concurrency: 5 }
      );

      return response;
    } catch (error) {
      throw error;
    }
  }

  /**
   * update docs by query
   * @param indexName
   * @param updateDoc
   * @param options
   */
  async function updateByQuery<T extends IUpdateDoc>(
    indexName: string,
    updateDoc: T,
    options?: Partial<RequestParams.UpdateByQuery>
  ): Promise<ActionResult> {
    try {
      const response = await client.updateByQuery({
        ...options,
        index: indexName,
        body: updateDoc,
      });

      const { statusCode, body } = response;
      const { updated, total } = body;

      const result = {
        _statusCode: statusCode,
        updated,
        total,
      } as ActionResult;
      return result;
    } catch (error) {
      throw error;
    }
  }

  /**
   * search docs
   * @param indexName 
   * @param query 
   * @param options 
   * @returns 
   */
  async function search<T = IBaseDoc>(
    indexName: string,
    query: Record<string, any>,
    options?: RequestParams.Search
  ): Promise<Array<T>> {
    const response = await client.search({
      ...options,
      index: indexName,
      body: query,
    });
    if (response.statusCode == 200) {
      // console.log(response.body.hits.hits);
      const result = response.body.hits.hits.map((doc: any) => {
        const { _index, _id, _seq_no, _primary_term, _score } = doc;
        return { _index, _id, _seq_no, _primary_term, _score, ...doc._source };
      });
      return result;
    } else {
      return [];
    }
  }

  /**
   * close es client
   */
  async function close() {
    await client.close();
  }
  return {
    client,
    indexMany,
    getById,
    deleteById,
    deleteByQuery,
    updateById,
    updateByQuery,
    search,
    exists,
    close,
  };
}

export type ElasticsearchWrapper = ReturnType<typeof createElasticWrapper>;
