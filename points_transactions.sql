SELECT uuid AS IdTransacao,
      id_customer AS IdCliente,
      created_at AS DtCriacao,
      vl_points AS QtdePontos,
      desc_sys_origin AS DescSistemaOrigem

FROM points.transactions