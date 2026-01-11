import {
  IExecuteFunctions,
  IDataObject,
  INodeExecutionData,
  NodeOperationError,
  IBinaryKeyData,
} from "n8n-workflow";

import { ResourceHandlerBase } from "./ResourceHandlerBase";
import { IResourceHandlerOptions } from "./ResourceHandlerFactory";
import {
  validateBinaryDataExists,
  bitrix24DownloadFile,
} from "../GenericFunctions";

/**
 * Xử lý các tác vụ File của Bitrix24
 */
export class FileResourceHandler extends ResourceHandlerBase {
  private readonly resourceEndpoints = {
    uploadToEntity: {
      "disk": "disk.storage.uploadfile",
      "folder": "disk.folder.uploadfile",
    },
    get: "disk.file.get",
    list: "disk.folder.getchildren",
    delete: "disk.file.delete",
  };

  constructor(
    executeFunctions: IExecuteFunctions,
    returnData: INodeExecutionData[],
    options: IResourceHandlerOptions = {}
  ) {
    super(executeFunctions, returnData, options);
  }

  /**
   * Xử lý tác vụ File
   */
  public async process(): Promise<INodeExecutionData[]> {
    for (let i = 0; i < this.items.length; i++) {
      try {
        const operation = this.getNodeParameter("operation", i) as string;

        switch (operation) {
          case "upload":
            await this.handleUpload(i);
            break;
          case "get":
            await this.handleGet(i);
            break;
          case "getAll":
            await this.handleGetAll(i);
            break;
          case "delete":
            await this.handleDelete(i);
            break;
          default:
            throw new NodeOperationError(
              this.executeFunctions.getNode(),
              `Không hỗ trợ tác vụ "${operation}" cho File`,
              { itemIndex: i }
            );
        }
      } catch (error) {
        if (this.executeFunctions.continueOnFail()) {
          this.returnData.push({ json: { error: error.message } });
          continue;
        }
        throw error;
      }
    }

    return this.returnData;
  }

  /**
   * Xử lý tham số tùy chỉnh
   */
  private processCustomParameters(
    options: IDataObject,
    params: IDataObject,
    itemIndex: number
  ): void {
    if (!options.customParameters) return;

    try {
      const customParams =
        typeof options.customParameters === "string"
          ? this.parseJsonParameter(
              options.customParameters as string,
              "Custom parameters phải là JSON hợp lệ",
              itemIndex
            )
          : options.customParameters;

      Object.assign(params, customParams);
    } catch (error) {
      throw new NodeOperationError(
        this.executeFunctions.getNode(),
        "Custom parameters phải là JSON hợp lệ",
        { itemIndex }
      );
    }
  }

  /**
   * Xử lý 'upload'
   */
  private async handleUpload(itemIndex: number): Promise<void> {
    const fileName = this.getNodeParameter(
      "fileName",
      itemIndex
    ) as string;

    const entityType = this.getNodeParameter(
      "entityType",
      itemIndex
    ) as string;

    const fileContentData = this.getNodeParameter(
      "fileContent",
      itemIndex
    ) as string;

    // Lấy options nếu có
    const options = this.getNodeParameter(
      "options",
      itemIndex,
      {}
    ) as IDataObject;

    // Upload lên disk storage
    const entityId = this.getNodeParameter("entityId", itemIndex) as string;
    const endpoint = this.resourceEndpoints.uploadToEntity[entityType as "disk" | "folder"];

    // Chuẩn bị request body theo format của Bitrix24 API
    // Format: { id, data: { NAME }, fileContent, ...otherParams }
    const requestBody: IDataObject = {
      id: entityId,
      data: {
        NAME: fileName,
      },
      fileContent: fileContentData
    };

    // Thêm các tham số tùy chọn từ options
    if (options.generateUniqueName !== undefined) {
      requestBody.generateUniqueName = options.generateUniqueName;
    }

    if (options.rights) {
      try {
        requestBody.rights =
          typeof options.rights === "string"
            ? this.parseJsonParameter(
                options.rights as string,
                "Rights phải là JSON hợp lệ",
                itemIndex
              )
            : options.rights;
      } catch (error) {
        // Ignore invalid JSON, sẽ được xử lý bởi parseJsonParameter
      }
    }

    // Thêm custom parameters nếu có (trừ các tham số đã xử lý ở trên)
    const customOptions = { ...options };
    delete customOptions.generateUniqueName;
    delete customOptions.rights;
    this.processCustomParameters(customOptions, requestBody, itemIndex);

    const responseData = await this.makeApiCall(
      endpoint,
      requestBody,
      {},
      itemIndex
    );
    this.addResponseToReturnData(responseData, itemIndex);
  }

  /**
   * Xử lý 'get'
   */
  private async handleGet(itemIndex: number): Promise<void> {
    const fileId = this.getNodeParameter("fileId", itemIndex) as string;
    const download = this.getNodeParameter(
      "download",
      itemIndex,
      false
    ) as boolean;
    const options = this.getNodeParameter(
      "options",
      itemIndex,
      {}
    ) as IDataObject;

    const queryParams: IDataObject = { id: fileId };
    this.processCustomParameters(options, queryParams, itemIndex);

    const responseData = await this.makeApiCall(
      this.resourceEndpoints.get,
      {},
      queryParams,
      itemIndex
    );

    // Nếu có yêu cầu download và có URL download trong phản hồi
    if (download && responseData?.result?.DOWNLOAD_URL) {
      const binaryPropertyName = this.getNodeParameter(
        "binaryPropertyName",
        itemIndex
      ) as string;
      const downloadUrl = responseData.result.DOWNLOAD_URL as string;
      const fileName = responseData.result.NAME as string;

      // Tải file
      const fileData = await bitrix24DownloadFile.call(
        this.executeFunctions,
        downloadUrl
      );
      const binaryData = await this.executeFunctions.helpers.prepareBinaryData(
        fileData as Buffer,
        fileName
      );

      // Tạo bản sao của item với dữ liệu nhị phân
      const newItem: INodeExecutionData = {
        json: responseData.result as IDataObject,
        binary: {
          [binaryPropertyName]: binaryData,
        },
      };

      this.addResponseToReturnData([newItem], itemIndex);
    } else {
      // Chỉ trả về thông tin file mà không download
      this.addResponseToReturnData(
        this.executeFunctions.helpers.returnJsonArray(
          responseData.result as IDataObject
        ),
        itemIndex
      );
    }
  }

  /**
   * Xử lý 'getAll'
   */
  private async handleGetAll(itemIndex: number): Promise<void> {
    const folderId = this.getNodeParameter("folderId", itemIndex) as string;
    const returnAll = this.getNodeParameter("returnAll", itemIndex) as boolean;
    const filter = this.getNodeParameter(
      "filter",
      itemIndex,
      {}
    ) as IDataObject;

    // Xây dựng tham số lọc
    const requestBody: IDataObject = {
      id: folderId,
    };

    if (filter) {
      try {
        requestBody.filter =
          typeof filter === "string"
            ? this.parseJsonParameter(filter as string, "Filter must be a valid JSON", itemIndex)
            : filter;
      } catch (error) {
        throw new NodeOperationError(
          this.executeFunctions.getNode(),
          "Filter must be a valid JSON",
          { itemIndex }
        );
      }
    }

    // Thêm custom parameters nếu có
    // this.processCustomParameters(filters, requestBody, itemIndex);

    const responseData = await this.makeApiCall(
      this.resourceEndpoints.list,
      requestBody,
      {},
      itemIndex,
      returnAll
    );

    this.addResponseToReturnData(responseData, itemIndex);
  }

  /**
   * Xử lý 'delete'
   */
  private async handleDelete(itemIndex: number): Promise<void> {
    const fileId = this.getNodeParameter("fileId", itemIndex) as string;
    const options = this.getNodeParameter(
      "options",
      itemIndex,
      {}
    ) as IDataObject;

    const queryParams: IDataObject = { id: fileId };
    this.processCustomParameters(options, queryParams, itemIndex);

    const responseData = await this.makeApiCall(
      this.resourceEndpoints.delete,
      {},
      queryParams,
      itemIndex
    );

    this.addResponseToReturnData(
      this.executeFunctions.helpers.returnJsonArray({
        success: responseData.result,
      } as IDataObject),
      itemIndex
    );
  }
}
