select 
    uuid AS idCliente,
    CASE WHEN desc_email IS NOT NULL THEN 1 ELSE 0 END AS flEmail,
    CASE WHEN id_twitch IS NOT NULL THEN 1 ELSE 0 END AS flTwitch,
    CASE WHEN id_you_tube IS NOT NULL THEN 1 ELSE 0 END AS flYouTube,
    CASE WHEN id_blue_sky IS NOT NULL THEN 1 ELSE 0 END AS flBlueSky,
    CASE WHEN id_instagram IS NOT NULL THEN 1 ELSE 0 END AS flInstagram,
    nr_points AS qtdePontos,
    case when created_at < '2021-01-01' then updated_at else created_at end as DtCriacao,
    updated_at AS DtAtualizacao

from points.customers